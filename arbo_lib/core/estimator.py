import numpy as np
from typing import Optional

from arbo_lib.db.store import ArboState
from arbo_lib.core.amdahl import AmdahlUtils
from arbo_lib.core.residual import ResidualModel
from arbo_lib.core.exceptions import TaskNotFoundError, TaskAlreadyExistsError
from arbo_lib.utils.logger import get_logger

logger = get_logger("arbo.estimator")

class ArboEstimator:
    def __init__(self):
        self.store = ArboState()
        self.residual_model = ResidualModel()

    def predict(self, task_name: str, input_quantity: float, cluster_load: float, max_time_slo: Optional[float] = None) -> tuple[int, float]:
        """
        Main Optimization loop, return optimal 's'
        :return:
        """
        params = self.store.get_task_model(task_name)

        # cold start
        if not params:
            logger.warning(f"'{task_name}' not found in DB. Triggering COLD START initialization.")
            self.store.initialize_task(task_name=task_name, t_base=0, base_input_quantity=input_quantity)
            return 1, 1.0

        # get baseline input quantity
        base_input_quantity = params["base_input_quantity"]
        gamma = input_quantity / base_input_quantity if base_input_quantity > 0 else 1.0

        # calibration run with moderately degree of parallelism
        if params["sample_count"] == 1:
            # TODO: make s adjustible via config
            logger.info(f"Calibration run for '{task_name}'; forcing s=5")
            return 5, gamma

        # train GP on last 50 executions
        history = self.store.get_history(task_name, limit=50)
        self.residual_model.train(history)

        # TODO: maybe scale max_s to make sure
        max_s = self._find_search_space(params["p_obs"])
        best_s = 1
        best_score = float("inf")

        candidates_s = np.arange(1, max_s * 1.2)

        logger.info(f"Searching for optimal s in range [{1}, {max_s*1.2}]")

        residuals = self.residual_model.predict(candidates_s, gamma, cluster_load)

        for i, s in enumerate(candidates_s):
            # calculate theoretical time
            t_amdahl = AmdahlUtils.calculate_theoretical_time(s=s, t_base=params["t_base_1"], c_startup=params["c_startup"], gamma=gamma, p=params["p_obs"])

            t_total = t_amdahl + residuals[i]

            # time constraint
            if max_time_slo and t_total > max_time_slo:
                continue

            cost = self._cost_function(t_total, s)

            if cost < best_score:
                best_score = cost
                best_s = s


        return int(best_s), gamma


    def feedback(self, task_name: str, s: int, gamma: float, cluster_load: float, t_actual: float) -> None:
        """
        Learning loop: updates parameters and saves execution
        :return:
        """
        params = self.store.get_task_model(task_name)

        # cold start
        if not params or params["sample_count"] == 0:
            logger.info(f"Initializing baseline metrics for '{task_name}' via feedback.")

            try:
                # self.store.initialize_task(task_name=task_name, t_base=t_actual)

                cost = self._cost_function(t_actual, s)
                run_data = self._pack_run_data(task_name, s, gamma, cluster_load, t_actual, residual=0, cost=cost, p_snapshot=1.0)
                self.store.update_baseline(task_name, t_actual)
                self.store.update_model(task_name, new_p=1, run_data=run_data)
                return
            except TaskAlreadyExistsError:
                # TODO: properly handle exception
                logger.warning(f"Task {task_name} already exists in DB")
                params = self.store.get_task_model(task_name)


        # infer p
        p_current = AmdahlUtils.calculate_current_p(
            s=s,
            t_actual=t_actual,
            c_startup=params["c_startup"],
            t_base=params["t_base_1"],
            gamma=gamma
        )

        # update p
        new_p = AmdahlUtils.update_p_moving_average(
            old_p=params["p_obs"],
            current_p=p_current,
            alpha=params["alpha"]
        )

        # calculate residual
        t_theory = AmdahlUtils.calculate_theoretical_time(
            s=s,
            t_base=params["t_base_1"],
            c_startup=params["c_startup"],
            gamma=gamma,
            p=new_p
        )

        residual = t_actual - t_theory
        cost = self._cost_function(t_actual, s)

        run_data = self._pack_run_data(task_name, s, gamma, cluster_load, t_actual, residual, cost, new_p)
        try:
            self.store.update_model(task_name, new_p=new_p, run_data=run_data)
        except TaskNotFoundError:
            logger.error(f"Task {task_name} not found in DB")


    def _cost_function(self, t: float, s: int):
        """
        Cost function to optimize
        :param t:
        :param s:
        :return:
        """
        return t * (s ** 0.5)

    def _find_search_space(self, p) -> int:
        return int(p / (1-p))


    def _pack_run_data(self, task, s, gamma, cluster_load, time, residual, cost, p_snapshot):
        """
        Helper to pack dictionary for storing
        :param task:
        :param s:
        :param gamma:
        :param cluster_load:
        :param time:
        :param residual:
        :param cost:
        :return:
        """
        return {
            "task_name": task,
            "s": s,
            "gamma": gamma,
            "cluster_load": cluster_load,
            "total_duration": time,
            "residual": residual,
            "cost_metric": cost,
            "p_snapshot": p_snapshot
        }
