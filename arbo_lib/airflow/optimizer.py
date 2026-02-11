from typing import List, Dict
import psutil
import boto3
from botocore.client import Config as botoConfig
from typing import Optional
import requests
from datetime import datetime
from urllib import parse

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
            actual_duration = self._get_task_group_metrics(dag_id, run_id, target_id)
        else:
            result = self._get_task_duration(dag_id, run_id, target_id)
            if result is not None:
                # TODO: properly handle start_up_time
                actual_duration, start_up_time = result

        if actual_duration is None:
            logger.info("Failed to query data, falling back to fallback duration")
            actual_duration = fallback_duration

        logger.info(f"Feedback '{task_name}': Actual={actual_duration:.2f}s vs Pred={predicted_amdahl + predicted_residual:.2f}s")

        self.estimator.feedback(task_name, s, gamma, cluster_load, actual_duration, predicted_amdahl, predicted_residual)

    @staticmethod
    def get_filesize(endpoint_url: str, access_key: str, secret_key: str, bucket_name: str, file_key: str) -> Optional[float]:
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
    def get_directory_size(endpoint_url: str, access_key: str, secret_key: str, bucket_name: str, prefix: str) -> Optional[float]:
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
        queued = datetime.strptime(data.get('queued_when'), self.fmt)
        start = datetime.strptime(data.get('start_date'), self.fmt)
        end = datetime.strptime(data.get('end_date'), self.fmt)

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

    def _get_task_group_metrics(self, dag_id: str, dag_run_id: str, group_id: str) -> Optional[float]:
        """
        Retrieves and calculates metrics for a specific task group within a DAG run.

        :param dag_id: The unique identifier of the DAG.
        :param dag_run_id: The unique identifier of the DAG run.
        :param group_id: The unique identifier of the task group whose metrics are to be retrieved.
        :return: The total wall-clock duration (in seconds) for all tasks in the specified group or None
            if no tasks are found for the group or if sufficient timing data is not available.
        """
        safe_run_id = parse.quote(dag_run_id)
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json"
        }

        # request to group endpoint
        url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns/{safe_run_id}/taskInstances"

        try:
            response = requests.get(url, headers=headers, timeout=5)
            response.raise_for_status()
            all_tasks = response.json().get("task_instances", [])

            # Filter for tasks belonging to group
            group_tasks = [
                t for t in all_tasks
                if t['task_id'] == group_id or t['task_id'].startswith(f"{group_id}.")
            ]

            if not group_tasks:
                logger.warning(f"No tasks found for group: {group_id}")
                return None

            start_times = [datetime.strptime(t['start_date'], self.fmt) for t in group_tasks if t.get('start_date')]
            end_times = [datetime.strptime(t['end_date'], self.fmt) for t in group_tasks if t.get('end_date')]

            if not start_times or not end_times:
                logger.warning("Group hasn't finished yet or tasks have no timing data.")
                return None

            group_start = min(start_times)
            group_end = max(end_times)
            total_duration = (group_end - group_start).total_seconds()

            logger.info(f"\n--- Task Group: {group_id} ---")
            logger.info(f"Total Tasks in Group: {len(group_tasks)}")
            logger.info(f"Group Start:          {group_start}")
            logger.info(f"Group End:            {group_end}")
            logger.info(f"Total Wall Duration:  {total_duration:.4f}s")

            return total_duration

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