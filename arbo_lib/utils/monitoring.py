import requests
import psutil
from arbo_lib.utils.logger import get_logger

logger = get_logger("arbo.monitoring")

class PrometheusClient:
    def __init__(self, namespace: str = "default"):
        self.prometheus_url = f"http://prometheus-server.{namespace}.svc.cluster.local/api/v1/query"

    def get_cluster_load(self) -> float:
        """Queries Prometheus for actual CPU utilization across the cluster."""
        query = '1 - (sum(rate(node_cpu_seconds_total{mode="idle"}[3m])) / sum(rate(node_cpu_seconds_total[3m])))'
        try:
            response = requests.get(self.prometheus_url, params={"query": query}, timeout=5)
            response.raise_for_status()
            data = response.json()
            results = data.get("data", {}).get("result", [])
            if results:
                value = float(results[0]['value'][1])
                logger.info(f"Prometheus Success: CPU Utilization is {value:.2f}")
                return value
        except Exception as e:
            logger.warning(f"Prometheus Query Failed ({e}). Falling back to local memory.")
        
        return self.get_local_memory_load()

    @staticmethod
    def get_local_memory_load() -> float:
        """Gets local virtual memory usage as a fallback."""
        mem = psutil.virtual_memory()
        return mem.percent / 100.0
