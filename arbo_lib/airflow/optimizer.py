from typing import List, Dict, Any
from arbo_lib.core.estimator import ArboEstimator

class ArboOptimizer:
    """
    Main entry point for Airflow DAGs; wraps estimator for easier usage
    """
    def __init__(self):
        self.estimator = ArboEstimator()

    def get_task_configs(self, task_name: str, gamma: float, cluster_load: float = 0.0, max_time_slo: float = None) -> List[Dict]:
        """
        Gets optimal value for 's' from estimator
        Returns a list of 's' configuration dictionaries for Airflow's dynamic task mapping
        :param task_name:
        :param gamma:
        :param cluster_load:
        :param max_time_slo:
        :return:
        """
        s_opt = self.estimator.predict(task_name=task_name, gamma=gamma, cluster_load=cluster_load, max_time_slo=max_time_slo)

        print(f"[{task_name}] Arbo Optimizer chose s={s_opt} (Gamma={gamma}, Load={cluster_load})")

        # create config list for dynamic task mapping
        configs = []
        for i in range(s_opt):
            configs.append({
                "chunk_id": i,
                "total_chunks": s_opt,
                "gamma": gamma,
                "task_name": task_name
            })

        return configs

    def report_success(self, task_name: str, total_duration: float, s: int, gamma: float, cluster_load: float):
        """
        callback after the parallel stage is done; feeds actual execution time into the DB
        :param task_name:
        :param total_duration:
        :param s:
        :param gamma:
        :param cluster_load:
        :return:
        """
        print(f"[{task_name}] Reporting Feedback to Arbo: s={s}, Time={total_duration:.2f}s")

        self.estimator.feedback(task_name, s, gamma, cluster_load, total_duration)