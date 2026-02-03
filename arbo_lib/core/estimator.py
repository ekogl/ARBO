import numpy as np
from typing import Optional
from math import ceil

from arbo_lib.db.store import ArboState
from arbo_lib.core.amdahl import AmdahlUtils
from arbo_lib.core.residual import ResidualModel
from arbo_lib.core.exceptions import TaskNotFoundError, TaskAlreadyExistsError, StaleDataError
from arbo_lib.utils.logger import get_logger

logger = get_logger("arbo.estimator")

class ArboEstimator:
    def __init__(self):
        self.store = ArboState()
        self.residual_model = ResidualModel()

    def predict(
            self, task_name: str, input_quantity: float, cluster_load: float, max_time_slo: Optional[float] = None
    ) -> tuple[int, float, float, float]:
        """
        Main Optimization loop, return optimal 's'
        :param task_name: Unique identifier for task
        :param input_quantity: metric representing the input size
        :param cluster_load: metric representing the cluster load
        :param max_time_slo: (Optional) maximum acceptable runtime in seconds
        :return: (best_s, gamma, predicted_amdahl, predicted_residual)
        """
        params = self.store.get_task_model(task_name)

        # cold start
        if not params:
            logger.warning(f"'{task_name}' not found in DB. Triggering COLD START initialization.")
            self.store.initialize_task(task_name=task_name, t_base=0, base_input_quantity=input_quantity)
            return 1, 1.0, 0.0, 0.0

        # get baseline input quantity
        base_input_quantity = params["base_input_quantity"]
        gamma = input_quantity / base_input_quantity if base_input_quantity > 0 else 1.0

        # calibration run with moderately degree of parallelism
        if params["sample_count"] == 1:
            # TODO: make s adjustible via config
            history = self.store.get_history(task_name, limit=10)  # limit is never actually reached
            self.residual_model.train(history)
            residuals = self.residual_model.predict(np.array([5]), gamma, cluster_load)
            predicted_amdahl = AmdahlUtils.calculate_theoretical_time(
                s=5,
                t_base=params["t_base_1"],
                c_startup=params["c_startup"],
                gamma=gamma,
                p=params["p_obs"],
                k=params["k_exponent"]
            )
            logger.info(f"Calibration run for '{task_name}'; forcing s=5")
            return 5, gamma, predicted_amdahl, float(self._sanitize_float(residuals[0]))

        # train GP on last 50 executions
        history = self.store.get_history(task_name, limit=50)
        self.residual_model.train(history)

        # TODO: maybe scale max_s to make sure  -- currently threshold is too low
        max_s = self._find_search_space(params["p_obs"])
        best_s = 1
        best_score = float("inf")

        predicted_amdahl = 0
        predicted_residual = 0

        candidates_s = np.arange(1, (ceil(max_s * 1.5)) + 1)

        logger.info(f"Searching for optimal s in range [{1}, {max_s*1.5}]")

        residuals = self.residual_model.predict(candidates_s, gamma, cluster_load)

        for i, s in enumerate(candidates_s):
            # calculate theoretical time
            t_amdahl = AmdahlUtils.calculate_theoretical_time(
                s=s, t_base=params["t_base_1"], c_startup=params["c_startup"],
                gamma=gamma, p=params["p_obs"], k=params["k_exponent"]
            )

            t_total = t_amdahl + residuals[i]

            # time constraint
            if max_time_slo and t_total > max_time_slo:
                continue

            cost = self._cost_function(t_total, s)

            if cost < best_score:
                best_score = cost
                best_s = s
                predicted_amdahl = self._sanitize_float(t_amdahl)
                predicted_residual = self._sanitize_float(residuals[i])


        return int(best_s), gamma, predicted_amdahl, predicted_residual


    def feedback(
            self, task_name: str, s: int, gamma: float, cluster_load: float, t_actual: float,
            predicted_amdahl: float, predicted_residual: float
    ) -> None:
        """
        Learning loop: updates parameters and saves execution
        :param task_name: Unique identifier for task
        :param s: degree of parallelism
        :param gamma: input scaling factor
        :param cluster_load: metric representing the cluster load
        :param t_actual: actual execution time of the task
        :param predicted_amdahl: predicted execution time based on Amdahl's Law
        :param predicted_residual: predicted residual (overhead) by the GP
        :return: None
        """

        max_retries = 3

        for attempt in range(max_retries):
            try:
                params = self.store.get_task_model(task_name)

                current_version = params["sample_count"] if params else 0

                # cold start
                if not params or params["sample_count"] == 0:
                    logger.info(f"Initializing baseline metrics for '{task_name}' via feedback.")

                    try:
                        cost = self._cost_function(t_actual, s)
                        run_data = self._pack_run_data(
                            task_name, s, gamma, cluster_load, t_actual, residual=0, cost=cost, p_snapshot=1.0,
                            predicted_amdahl=predicted_amdahl, predicted_residual=predicted_residual
                        )
                        self.store.update_baseline(task_name, t_actual)
                        self.store.update_model(task_name, new_p=1, new_k=1, run_data=run_data, expected_version=0)
                        return
                    except TaskAlreadyExistsError:
                        # TODO: properly handle exception
                        logger.warning(f"Task {task_name} already exists in DB")
                        params = self.store.get_task_model(task_name)
                    except StaleDataError:
                        continue

                current_k = params["k_exponent"]

                # infer p
                p_current = AmdahlUtils.calculate_current_p(
                    s=s,
                    t_actual=t_actual,
                    c_startup=params["c_startup"],
                    t_base=params["t_base_1"],
                    gamma=gamma,
                    k=current_k
                )

                # update p
                new_p = AmdahlUtils.update_moving_average(
                    old_val=params["p_obs"],
                    current_val=p_current,
                    alpha=params["alpha_p"]
                )

                k_current = AmdahlUtils.calculate_current_k(
                    s=s,
                    t_base=params["t_base_1"],
                    t_actual=t_actual,
                    c_startup=params["c_startup"],
                    gamma=gamma,
                    p=new_p
                )

                alpha_k = params["alpha_k"]
                new_k = AmdahlUtils.update_moving_average(
                    old_val=current_k,
                    current_val=k_current,
                    alpha=alpha_k
                )

                # calculate residual
                t_theory = AmdahlUtils.calculate_theoretical_time(
                    s=s,
                    t_base=params["t_base_1"],
                    c_startup=params["c_startup"],
                    gamma=gamma,
                    p=new_p,
                    k=new_k
                )

                residual = t_actual - t_theory
                cost = self._cost_function(t_actual, s)

                run_data = self._pack_run_data(
                    task_name, s, gamma, cluster_load, t_actual,
                    residual, cost, new_p, predicted_amdahl, predicted_residual
                )
                self.store.update_model(
                    task_name, new_p=new_p, run_data=run_data, new_k=new_k, expected_version=current_version
                )

                return

            except StaleDataError:
                logger.warning(f"Optimistic Lock Conflict for {task_name}. Retrying ({attempt + 1}/{max_retries})...")
                continue
            except TaskNotFoundError:
                logger.error(f"Task {task_name} disappeared during feedback.")
                continue

        logger.error(f"Failed to update model for {task_name} after {max_retries} retries due to concurrency.")


    @staticmethod
    def _cost_function(t: float, s: int) -> float:
        """
        Cost function to optimize
        :param t: predicted execution time
        :param s: degree of parallelism
        :return: cost of configuration
        """
        return t * (s ** 0.5)

    @staticmethod
    def _find_search_space(p) -> int:
        """
        Finds upper bound for search space
        :param p: parallelizable part of task
        :return: upper bound for search space
        """
        if p >= 0.99:
            return 50  # to avoid 0 division error

        limit = ceil(p / (1-p))

        return max(limit, 15)  # search up to at least 15 workers

    @staticmethod
    def _sanitize_float(x: float) -> float:
        """
        Safely cast to float and clamp
        :param x: input value
        :return: casted value to float
        """
        try:
            f_val = float(x)
            if f_val == 0.0:
                return 0.0
            if abs(f_val) < 1e-10:
                return 0.0
            if abs(f_val) > 1e10:
                return 1e10 if f_val > 0 else -1e10
            return f_val
        except ValueError:
            return 0.0


    @staticmethod
    def _pack_run_data(
            task, s, gamma, cluster_load, time, residual, cost, p_snapshot, predicted_amdahl, predicted_residual
    ) -> dict:
        """
        Helper to pack dictionary for storing
        :param task: Unique identifier for task
        :param s: degree of parallelism
        :param gamma: input scaling factor
        :param cluster_load: metric representing cluster load
        :param time: actual execution time of the task
        :param residual: actual residual (overhead) by the GP
        :param cost: cost of the configuration
        :param p_snapshot: snapshot of p at the time of feedback
        :param predicted_amdahl: predicted execution time based on Amdahl's Law
        :param predicted_residual: predicted residual (overhead) by the GP
        :return: dictionary containing all run data
        """
        return {
            "task_name": task,
            "s": s,
            "gamma": gamma,
            "cluster_load": cluster_load,
            "total_duration": time,
            "residual": residual,
            "cost_metric": cost,
            "p_snapshot": p_snapshot,
            "time_amdahl": predicted_amdahl,
            "pred_residual": predicted_residual
        }
