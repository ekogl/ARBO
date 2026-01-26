import numpy as np
from typing import List, Dict, Optional

from arbo_lib.db.store import ArboState
from arbo_lib.core.amdahl import AmdahlUtils
from arbo_lib.core.residual import ResidualModel
from arbo_lib.core.exceptions import TaskNotFoundError, TaskAlreadyExistsError

class ArboEstimator:
    def __init__(self):
        self.store = ArboState()
        self.residual_model = ResidualModel()

    def predict(self, task_name: str, gamma: float, cluster_load: float, max_time_slo: Optional[float] = None) -> int:
        """
        Main Optimization loop, return optimal 's'
        :return:
        """
        params = self.store.get_task_model(task_name)

        # cold start
        if not params:
            print(f"Cold Start for task {task_name}, forcing s=1")
            return 1

        # TODO: check if sample_count = 1 -> baseline done, might need additional mildly parallel run to estimate p

        # train GP on last 50 executions
        history = self.store.get_history(task_name, limit=50)
        self.residual_model.train(history)

        # TODO: smart way to figure out search space [1, x] -> maybe based on p?
        best_s = 1
        best_score = float("inf")

        candidates_s = np.arange(1, 21)

        residuals = self.residual_model.predict(candidates_s, gamma, cluster_load)

        for i, s in enumerate(candidates_s):
            # calculate theoretical time
            t_amdahl = AmdahlUtils.calculate_theoretical_time(s=s, t_base=params["t_base_1"], c_startup=params["c_startup"], gamma=gamma, p=params["p_obs"])

            t_total = t_amdahl + residuals[i]

            # time constraint
            if max_time_slo and t_total > max_time_slo:
                continue

            # TODO: implement proper cost function
            cost = t_total * s

            if cost < best_score:
                best_score = cost
                best_s = s

        return int(best_s)


    def feedback(self, task_name: str, s: int, gamma: float, cluster_load: float, t_actual: float) -> None:
        """
        Learning loop: updates parameters and saves execution
        :return:
        """
        params = self.store.get_task_model(task_name)

        # cold start
        if not params:
            print(f"Initializing {task_name}")

            try:
                self.store.initialize_task(task_name=task_name, t_base=t_actual)

                # TODO: implement proper cost function
                cost = t_actual * 1
                run_data = self._pack_run_data(task_name, s, gamma, cluster_load, t_actual, residual=0, cost=cost)
                self.store.update_model(task_name, new_p=1, run_data=run_data)
                return
            except TaskAlreadyExistsError:
                # TODO: properly handle exception
                print(f"Task {task_name} already exists in DB")
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

            # TODO: update cost metric
            cost = t_actual * s
            run_data = self._pack_run_data(task_name, s, gamma, cluster_load, t_actual, residual, cost)

            try:
                self.store.update_model(task_name, new_p=new_p, run_data=run_data)
            except TaskNotFoundError:
                print(f"Task {task_name} not found in DB")





    def _pack_run_data(self, task, s, gamma, cluster_load, time, residual, cost):
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
            "cost_metric": cost
        }
