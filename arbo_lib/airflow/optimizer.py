from typing import List, Dict
import psutil
import boto3
from botocore.client import Config as botoConfig
from typing import Optional
import requests
from datetime import datetime
from urllib import parse
import re

from arbo_lib.core.estimator import ArboEstimator
from arbo_lib.config import Config
from arbo_lib.utils.logger import get_logger

logger = get_logger("arbo.optimizer")


class ArboOptimizer:
    """
    Main entry point for Airflow DAGs; wraps estimator for easier usage
    """

    def __init__(self, namespace: str, is_local: bool = False):
        self.estimator = ArboEstimator()

        self.namespace = namespace

        if is_local:
            self.base_url = "http://localhost:8080"
        else:
            self.base_url = f"http://airflow-api-server.{self.namespace}.svc.cluster.local:8080"

        self.username = Config.AIRFLOW_USER
        self.password = Config.AIRFLOW_PASS
        self.bearer_token = None
        self.fmt = "%Y-%m-%dT%H:%M:%S.%fZ"

    @staticmethod
    def _compare_run_ids(id1: str, id2: str) -> bool:
        """
        Compares two run IDs by sanitizing them (removing non-alphanumeric characters and lowercasing).
        This helps matching Airflow run_ids with sanitized Kubernetes labels.
        """

        def sanitize(s):
            return re.sub(r'[^a-zA-Z0-9]', '', s).lower()

        return sanitize(id1) == sanitize(id2)

    def get_task_configs(self, task_name: str, input_quantity: float, cluster_load: float = 0.0,
                         max_time_slo: float = None
                         ) -> List[Dict]:
        """
        Gets optimal value for 's' from estimator
        Returns a list of 's' configuration dictionaries for Airflow's dynamic task mapping
        :param task_name: Unique identifier for task
        :param input_quantity: metric representing input size
        :param cluster_load: metric representing the cluster load
        :param max_time_slo: (Optional) maximum acceptable runtime in seconds
        :return: list of dictionaries, where the length of the list is 's' and each dictionary contains the configuration for a single worker
        """
        s_opt, calculated_gamma, predicted_amdahl, predicted_residual = self.estimator.predict(
            task_name=task_name,
            input_quantity=input_quantity,
            cluster_load=cluster_load,
            max_time_slo=max_time_slo
        )

        logger.info(f"Request received for '{task_name}': Input Quantity={input_quantity}, Load={cluster_load}")

        # create config list for dynamic task mapping
        configs = []
        for i in range(s_opt):
            configs.append({
                "chunk_id": i,
                "total_chunks": s_opt,
                "gamma": calculated_gamma,
                "task_name": task_name,
                "amdahl_time": predicted_amdahl,
                "residual_prediction": predicted_residual
            })

        return configs

    def report_success(
            self, task_name: str, s: int, gamma: float, cluster_load: float,
            predicted_amdahl: float, predicted_residual: float, dag_id: str, run_id: str,
            target_id: str, fallback_duration: float, is_group: bool
    ) -> None:
        """
        callback after the parallel stage is done; feeds actual execution time into the DB
        :param task_name: Unique identifier for task
        :param s: degree of parallelism (number of workers)
        :param gamma: input scaling factor
        :param cluster_load: metric representing cluster load
        :param predicted_amdahl: predicted execution time based on Amdahl's Law
        :param predicted_residual: predicted residual (overhead) by the GP
        :param dag_id: DAG ID
        :param run_id: DAG run ID
        :param target_id: Task ID or Task Group ID
        :param fallback_duration: fallback duration in case of failure
        :param is_group: True if the target is a task group, False otherwise
        :return: None
        """
        self.bearer_token = self._get_bearer_token()
        actual_duration = None

        if is_group:
            result = self._get_task_group_metrics(dag_id, run_id, target_id)
        else:
            result = self._get_task_duration(dag_id, run_id, target_id)

        if result is not None:
            actual_duration, start_up_time = result

            if is_group:
                pure_execution_duration = actual_duration - start_up_time
            else:
                pure_execution_duration = actual_duration
        else:
            logger.info("Failed to query data, falling back to fallback duration")
            pure_execution_duration = fallback_duration
            start_up_time = 0.0

        logger.info(
            f"Feedback '{task_name}': Exec={pure_execution_duration:.2f}s (K8s Overhead={start_up_time:.2f}s) vs Pred={predicted_amdahl + predicted_residual:.2f}s")

        self.estimator.feedback(task_name, s, gamma, cluster_load, actual_duration, predicted_amdahl,
                                predicted_residual, start_up_time)

    @staticmethod
    def get_filesize(endpoint_url: str, access_key: str, secret_key: str, bucket_name: str, file_key: str) -> Optional[
        float]:
        """
        Queries MinIO for file size
        :param endpoint_url: MinIO endpoint URL
        :param access_key: MinIO access key
        :param secret_key: MinIO secret key
        :param bucket_name: MinIO bucket name
        :param file_key: MinIO file key
        :return: size of the file in bytes, None if the query failed
        """
        try:
            s3 = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=botoConfig(signature_version='s3v4')
            )
            obj = s3.head_object(Bucket=bucket_name, Key=file_key)
            input_quantity = obj["ContentLength"]
            logger.info(f"MinIO Query Success: Input Size is {input_quantity} bytes")
            return input_quantity
        except Exception as e:
            logger.warning(f"MinIO Query Failed ({e}). Returning None")
            return None

    @staticmethod
    def get_directory_size(endpoint_url: str, access_key: str, secret_key: str, bucket_name: str, prefix: str) -> \
    Optional[float]:
        """
        Queries MinIO for directory size
        :param endpoint_url: MinIO endpoint URL
        :param access_key: MinIO access key
        :param secret_key: MinIO secret key
        :param bucket_name: MinIO bucket name
        :param prefix: MinIO directory prefix
        :return: size of the directory in bytes, None if the query failed
        """
        try:
            s3 = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=botoConfig(signature_version='s3v4')
            )
            paginator = s3.get_paginator("list_objects_v2")

            total_size = 0

            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        total_size += obj["Size"]
            input_quantity = total_size
            logger.info(f"MinIO Query Success: Input Size is {input_quantity} bytes")
            return input_quantity
        except Exception as e:
            logger.warning(f"MinIO Query Failed ({e}). Returning None")
            return None

    def get_cluster_load(self, namespace: str = "default", ) -> float:
        """
        Queries Prometheus for actual CPU utilization across the cluster.
        :param namespace: namespace of the Prometheus instance
        :return:
        """
        prometheus_url = f"http://prometheus-server.{namespace}.svc.cluster.local/api/v1/query"
        query = '1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m]))'

        try:
            response = requests.get(prometheus_url, params={"query": query}, timeout=5)
            results = response.json()["data"]["result"]
            if results:
                logger.info(f"Prometheus Query Success: CPU Utilization is {results[0]['value'][1]}")
                return float(results[0]['value'][1])
            else:
                logger.warning("Prometheus Query Failed")
        except Exception as e:
            logger.warning(f"Prometheus Query Failed ({e})")

        # TODO: properly handle failure
        return self.get_virtual_memory()

    @staticmethod
    def _parse_iso(dt_str: str) -> datetime:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))

    def _get_task_duration(self, dag_id: str, dag_run_id: str, task_id: str) -> Optional[tuple[float, float]]:
        """
        Retrieve the duration and startup time of a specific task instance in a DAG run.

        :param dag_id: The unique identifier of the DAG.
        :param dag_run_id: The unique identifier of the DAG run.
        :param task_id: The unique identifier of the task within the DAG.
        :return: A tuple containing the task duration (in seconds) and startup time
                 (in seconds) or None if the request fails.
        """
        safe_run_id = parse.quote(dag_run_id)
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json"
        }
        url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns/{safe_run_id}/taskInstances/{task_id}"

        try:
            response = requests.get(url, headers=headers, timeout=5)
            response.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to get task duration ({e})")
            return None

        data = response.json()
        duration = data.get("duration")
        queued = self._parse_iso(data.get('queued_when'))
        start = self._parse_iso(data.get('start_date'))
        end = self._parse_iso(data.get('end_date'))

        startup_time = (start - queued).total_seconds()

        if duration is None or startup_time is None:
            logger.warning("Failed to compute task duration or startup time")
            return None

        logger.info(f"\n--- Task: {task_id} ---")
        logger.info(f"Task Start:          {start}")
        logger.info(f"Task End:            {end}")
        logger.info(f"Total Wall Duration:  {duration:.4f}s")
        logger.info(f"Startup Time:         {startup_time:.4f}s")

        return duration, startup_time

    def _get_task_group_metrics(self, dag_id: str, dag_run_id: str, group_id: str) -> Optional[tuple[float, float]]:
        """
        Retrieves and calculates metrics for a specific task group within a DAG run.
        Includes K8s startup overhead (scheduling, image pulling) by querying Prometheus.
        """
        safe_run_id = parse.quote(dag_run_id)
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json"
        }

        # 1. Get task instances from Airflow API
        params = {"limit": 1000}
        url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns/{safe_run_id}/taskInstances"

        try:
            response = requests.get(url, headers=headers, timeout=5, params=params)
            response.raise_for_status()
            all_tasks = response.json().get("task_instances", [])

            # Filter for tasks belonging to group
            group_tasks = [
                t for t in all_tasks
                if t["task_id"] == group_id or t["task_id"].startswith(f"{group_id}.")
            ]

            if not group_tasks:
                logger.warning(f"No tasks found for group: {group_id}")
                return None

            queued_times = [self._parse_iso(t["queued_when"]) for t in group_tasks if t.get("queued_when")]
            end_times = [self._parse_iso(t["end_date"]) for t in group_tasks if t.get("end_date")]

            if not queued_times or not end_times:
                logger.warning("Group hasn't finished yet or tasks have no timing data.")
                return None

            group_queued = min(queued_times)
            group_end = max(end_times)
            total_wall_duration = (group_end - group_queued).total_seconds()

            # 2. Query Prometheus for K8s startup overhead
            # Sanitize group_id for Prometheus label matching (dots/underscores often become dashes)
            sanitized_group_prefix = re.sub(r'[^a-zA-Z0-9]', '.*', group_id)
            prometheus_url = f"http://prometheus-server.{self.namespace}.svc.cluster.local/api/v1/query"
            prom_query = (
                f'(max_over_time(kube_pod_container_state_started{{namespace="{self.namespace}", container="base"}}[1h]) '
                f' - ignoring(container) max_over_time(kube_pod_created{{namespace="{self.namespace}"}}[1h])) '
                f'* on(pod, namespace) group_left(label_dag_id, label_task_id, label_run_id, label_map_index) '
                f'max_over_time(kube_pod_labels{{namespace="{self.namespace}", label_dag_id="{dag_id}", label_task_id=~"{sanitized_group_prefix}.*"}}[1h])'
            )

            k8s_overheads = {}
            try:
                prom_resp = requests.get(prometheus_url, params={"query": prom_query}, timeout=10)
                prom_resp.raise_for_status()
                results = prom_resp.json().get("data", {}).get("result", [])

                logger.info(f"Raw Prometheus Results: {results}")

                for r in results:
                    metric = r.get("metric", {})
                    pod_run_id = metric.get("label_run_id")
                    if pod_run_id and self._compare_run_ids(pod_run_id, dag_run_id):
                        t_id = metric.get("label_task_id")
                        m_idx = int(metric.get("label_map_index", -1))
                        val = float(r.get("value", [0, 0])[1])
                        k8s_overheads[(t_id, m_idx)] = val
                        logger.debug(f"Prometheus: Task {t_id}[{m_idx}] K8s Overhead: {val:.2f}s")
            except Exception as e:
                logger.warning(f"Prometheus query failed: {e}")

            # 3. Calculate total startup overhead
            map_index_startups = {}
            logger.info(f"\n--- Metrics for Group: {group_id} ---")
            for t in group_tasks:
                if t.get("start_date") and t.get("queued_when"):
                    airflow_start = self._parse_iso(t["start_date"])
                    airflow_queued = self._parse_iso(t["queued_when"])
                    airflow_delay = (airflow_start - airflow_queued).total_seconds()

                    mi = t.get("map_index", -1)
                    # Try to find K8s overhead (accounting for label sanitization in matching)
                    k8s_val = 0.0
                    for (task_id_key, m_idx_key), val in k8s_overheads.items():
                        if m_idx_key == mi and (
                                task_id_key == t["task_id"] or task_id_key.replace("-", "_") == t["task_id"].replace(
                                ".", "_")):
                            k8s_val = val
                            break

                    total_task_startup = airflow_delay + k8s_val
                    map_index_startups[mi] = map_index_startups.get(mi, 0.0) + total_task_startup

                    logger.info(
                        f"  Task: {t['task_id']:.<40} Index: {mi:<3} | AF Delay: {airflow_delay:>6.2f}s | K8s: {k8s_val:>6.2f}s | Total: {total_task_startup:>6.2f}s")

            if map_index_startups:
                avg_group_startup_time = sum(map_index_startups.values()) / len(map_index_startups)
                logger.info(
                    f"Summary: Wall={total_wall_duration:.2f}s, Avg Startup={avg_group_startup_time:.2f}s (across {len(map_index_startups)} indices)")
            else:
                avg_group_startup_time = 0.0
                logger.warning("No timing data available for any tasks in the group.")

            return total_wall_duration, avg_group_startup_time

        except Exception as e:
            logger.error(f"Failed to get group metrics ({e})")
            return None

        except Exception as e:
            logger.error(f"Failed to get group metrics ({e})")
            return None

    def _get_bearer_token(self) -> Optional[str]:
        """
        Gets bearer token for Airflow API
        :return: bearer token, None if authentication failed
        """
        auth_url = f"{self.base_url}/auth/token"
        payload = {
            "username": self.username,
            "password": self.password
        }
        try:
            response = requests.post(auth_url, json=payload, timeout=5)
            response.raise_for_status()
            return response.json().get("access_token")
        except Exception as e:
            logger.warning(f"Authentication Failed ({e})")
            return None

    @staticmethod
    def get_virtual_memory() -> float:
        """
        Gets virtual memory usage, returns memory usage
        NOTE: This should not be used in production settings

        :return: Memory usage as a normalized float (0.0 to 1.0
        """
        mem = psutil.virtual_memory()
        return mem.percent / 100