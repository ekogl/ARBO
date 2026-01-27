from typing import List, Dict, Any
from arbo_lib.core.estimator import ArboEstimator
from arbo_lib.utils.logger import get_logger

logger = get_logger("arbo.optimizer")

class ArboOptimizer:
    """
    Main entry point for Airflow DAGs; wraps estimator for easier usage
    """
    def __init__(self):
        self.estimator = ArboEstimator()

    # TODO: handle cluster load properly
    def get_task_configs(self, task_name: str, input_quantity: float, cluster_load: float = 0.0, max_time_slo: float = None) -> List[Dict]:
        """
        Gets optimal value for 's' from estimator
        Returns a list of 's' configuration dictionaries for Airflow's dynamic task mapping
        :param input_quantity:
        :param task_name:
        :param cluster_load:
        :param max_time_slo:
        :return:
        """
        s_opt, calculated_gamma = self.estimator.predict(task_name=task_name, input_quantity=input_quantity, cluster_load=cluster_load, max_time_slo=max_time_slo)

        logger.info(f"Request received for '{task_name}': Input Quantity={input_quantity}, Load={cluster_load}")

        # create config list for dynamic task mapping
        configs = []
        for i in range(s_opt):
            configs.append({
                "chunk_id": i,
                "total_chunks": s_opt,
                "gamma": calculated_gamma,
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
        logger.info(
            f"Feedback received for '{task_name}': "
            f"s={s}, Time={total_duration:.2f}s, Gamma={gamma:.2f}"
        )

        self.estimator.feedback(task_name, s, gamma, cluster_load, total_duration)