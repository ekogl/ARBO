from typing import List, Dict, Optional
from arbo_lib.core.estimator import ArboEstimator
from arbo_lib.utils.logger import get_logger
from arbo_lib.utils.storage import MinioClient
from arbo_lib.utils.monitoring import PrometheusClient
from arbo_lib.airflow.collector import AirflowMetricCollector

logger = get_logger("arbo.optimizer")


class ArboOptimizer:
    """
    Main orchestrator for Airflow DAGs.
    Coordinates metric collection, cluster monitoring, and parallelism estimation.
    """

    def __init__(self, namespace: str, is_local: bool = False):
        self.namespace = namespace
        self.base_url = "http://localhost:8080" if is_local else f"http://airflow-api-server.{namespace}.svc.cluster.local:8080"

        self.estimator = ArboEstimator()
        self.storage = MinioClient()
        self.monitoring = PrometheusClient(namespace)
        self.collector = AirflowMetricCollector(self.base_url, namespace)

    def get_task_configs(self, task_name: str, input_quantity: float, cluster_load: float = 0.0,
                         max_time_slo: float = None) -> List[Dict]:
        """Gets optimal value for 's' from estimator."""
        s_opt, gamma, t_amdahl, t_resid = self.estimator.predict(
            task_name=task_name, input_quantity=input_quantity,
            cluster_load=cluster_load, max_time_slo=max_time_slo
        )
        logger.info(f"Optimization for '{task_name}': s={s_opt}, gamma={gamma:.2f}")
        return [{
            "chunk_id": i, "total_chunks": s_opt, "gamma": gamma,
            "task_name": task_name, "amdahl_time": t_amdahl, "residual_prediction": t_resid
        } for i in range(s_opt)]

    def report_success(self, task_name: str, s: int, gamma: float, cluster_load: float,
                       predicted_amdahl: float, predicted_residual: float, dag_id: str, run_id: str,
                       target_id: str, fallback_duration: float, is_group: bool) -> None:
        """Callback after execution; feeds actual timing data back into the model."""
        if is_group:
            result = self.collector.get_group_metrics(dag_id, run_id, target_id)
        else:
            result = self.collector.get_task_metrics(dag_id, run_id, target_id)

        if result:
            t_total, overhead, pull = result
            exec_time = max(0.1, t_total - overhead)
        else:
            logger.warning(f"Metric collection failed for {target_id}. Using fallback.")
            t_total, overhead, pull, exec_time = fallback_duration, 0.0, 0.0, fallback_duration

        logger.info(f"Feedback '{task_name}': Exec={exec_time:.2f}s (Overhead={overhead:.2f}s, Pull={pull:.2f}s)")

        self.estimator.feedback(
            task_name=task_name, s=s, gamma=gamma, cluster_load=cluster_load,
            t_actual=t_total, predicted_amdahl=predicted_amdahl,
            predicted_residual=predicted_residual, dynamic_c_startup=overhead, pull_time=pull
        )

    # --- Wrapper methods for backward compatibility with DAGs ---
    def get_filesize(self, *args, **kwargs):
        return self.storage.get_filesize(*args, **kwargs)

    def get_directory_size(self, *args, **kwargs):
        return self.storage.get_directory_size(*args, **kwargs)

    def get_cluster_load(self, *args, **kwargs):
        return self.monitoring.get_cluster_load()

    def get_virtual_memory(self, *args, **kwargs):
        return self.monitoring.get_local_memory_load()
