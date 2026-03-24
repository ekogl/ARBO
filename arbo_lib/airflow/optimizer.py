from typing import List, Dict
import psutil
import boto3
from botocore.client import Config as botoConfig
from typing import Optional
import requests
from datetime import datetime
from urllib import parse
import re
from kubernetes import client, config
from collections import defaultdict
from kubernetes.client.exceptions import ApiException

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

        if is_group:
            result = self._get_task_group_metrics(dag_id, run_id, target_id)
        else:
            result = self._get_task_duration(dag_id, run_id, target_id)

        if result is not None:
            actual_duration, start_up_time, pull_time = result
            if is_group:
                # Group duration from _get_task_group_metrics already includes startup overhead (min_queued to max_end)
                t_total = actual_duration
                pure_execution_duration = actual_duration - start_up_time
            else:
                # Individual duration from _get_task_duration is (end - start), so we add start_up_time to get total
                t_total = actual_duration + start_up_time
                pure_execution_duration = actual_duration
        else:
            logger.info("Failed to query data, falling back to fallback duration")
            t_total = fallback_duration
            pure_execution_duration = fallback_duration
            start_up_time = 0.0
            pull_time = 0.0



        logger.info(f"Feedback '{task_name}': Exec={pure_execution_duration:.2f}s (K8s Overhead={start_up_time:.2f}s, Pull={pull_time:.2f}s) vs Pred={predicted_amdahl + predicted_residual:.2f}s")

        self.estimator.feedback(
            task_name=task_name, s=s, gamma=gamma, cluster_load=cluster_load,
            t_actual=t_total, predicted_amdahl=predicted_amdahl,
            predicted_residual=predicted_residual, dynamic_c_startup=start_up_time,
            pull_time=pull_time
        )

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
        # TODO: change query
        prometheus_url = f"http://prometheus-server.{namespace}.svc.cluster.local/api/v1/query"
        # TODO: fix query (works with cAdvisor)
        # query = '1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m]))'
        query = '1 - (sum(rate(node_cpu_seconds_total{mode="idle"}[5m])) / sum(rate(node_cpu_seconds_total[5m])))'

        try:
            response = requests.get(prometheus_url, params={"query": query}, timeout=5)
            response.raise_for_status()
            data = response.json()
            results = data.get("data", {}).get("result", [])
            if results:
                value = results[0]['value'][1]
                logger.info(f"Prometheus Query Success: CPU Utilization is {value}")
                return float(value)
            else:
                logger.warning("Prometheus Query Succeeded but no results found.")
        except requests.exceptions.RequestException as e:
                logger.warning(f"Prometheus HTTP Request Failed: {e}")
        except (KeyError, IndexError, ValueError) as e:
            logger.warning(f"Failed to parse Prometheus response: {e}")
        except Exception as e:
            logger.warning(f"Unexpected error querying Prometheus: {e}")

        # TODO: properly handle failure
        return self.get_virtual_memory()

    @staticmethod
    def _parse_iso(dt_str: str) -> datetime:
        # TODO: docu
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))


    # TODO: check if actually used
    def _get_task_duration(self, dag_id: str, dag_run_id: str, task_id: str) -> Optional[tuple[float, float, float]]:
        """
        Retrieve the duration, startup time, and pull time of a specific task instance in a DAG run.

        :param dag_id: The unique identifier of the DAG.
        :param dag_run_id: The unique identifier of the DAG run.
        :param task_id: The unique identifier of the task within the DAG.
        :return: A tuple containing the task duration (in seconds), startup time
                 (in seconds), and pull time (in seconds) or None if the request fails.
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

        airflow_delay = (start - queued).total_seconds()

        # resolve pod name and get k8s events
        pod_name = self._resolve_pod_name(dag_id, dag_run_id, task_id, -1)
        k8s_startup = 0.0
        pull_time = 0.0

        if pod_name:
            lifecycle = self._get_pod_lifecycle_events(pod_name)
            if "scheduled" in lifecycle and "started" in lifecycle:
                k8s_startup = lifecycle["started"] - lifecycle["scheduled"]
                if "pull_duration" in lifecycle:
                    pull_time = lifecycle["pull_duration"]
                elif "pulling" in lifecycle and "pulled" in lifecycle:
                    pull_time = lifecycle["pulled"] - lifecycle["pulling"]

        total_startup_time = airflow_delay + k8s_startup

        if duration is None:
            logger.warning("Failed to compute task duration")
            return None

        logger.info(f"\n--- Task: {task_id} ---")
        logger.info(f"Task Start:          {start}")
        logger.info(f"Task End:            {end}")
        logger.info(f"Total Wall Duration:  {duration:.4f}s")
        logger.info(f"Total Startup Time:   {total_startup_time:.4f}s (K8s: {k8s_startup:.2f}s, Pull: {pull_time:.2f}s)")

        return duration, total_startup_time, pull_time

    def _get_task_group_metrics(self, dag_id: str, dag_run_id: str, group_id: str) -> Optional[tuple[float, float, float]]:
        """
        Retrieves and calculates metrics for a specific task group within a DAG run.
        Includes K8s startup overhead (scheduling, image pulling) by querying Prometheus.
        Returns: (total_wall_duration, avg_group_startup_time, avg_pull_time)
        """
        safe_run_id = parse.quote(dag_run_id)
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json"
        }

        # get task instances from airflow api
        params = {"limit": 1000}
        url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns/{safe_run_id}/taskInstances"

        try:
            response = requests.get(url, headers=headers, timeout=5, params=params)
            response.raise_for_status()
            all_tasks = response.json().get("task_instances", [])
        except Exception as e:
            logger.error(f"Failed to get task instances ({e})")
            return None

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

        # for each task resolve name and retrieve lifecycle
        logger.info(f"\n--- Group Metrics for {group_id} ---")

        map_index_startups: Dict[int, float] = defaultdict(float)
        map_index_pulls: Dict[int, float] = defaultdict(float)

        for t in group_tasks:
            if not (t.get("start_date") and t.get("end_date")):
                continue

            task_id = t["task_id"]
            mi = t.get("map_index", -1)

            airflow_queued = self._parse_iso(t["queued_when"])
            airflow_start = self._parse_iso(t["start_date"])
            airflow_delay = (airflow_start - airflow_queued).total_seconds()

            pod_name = self._resolve_pod_name(dag_id, dag_run_id, task_id, mi)

            k8s_startup = 0.0
            pull_time = 0.0

            if pod_name:
                lifecycle = self._get_pod_lifecycle_events(pod_name)

                if "scheduled" in lifecycle and "started" in lifecycle:
                    k8s_startup = lifecycle["started"] - lifecycle["scheduled"]

                    if "pull_duration" in lifecycle:
                        pull_time = lifecycle["pull_duration"]
                    elif "pulling" in lifecycle and "pulled" in lifecycle:
                        pull_time = lifecycle["pulled"] - lifecycle["pulling"]
                else:
                    logger.warning(f"Pod {pod_name} has no lifecycle events. Falling back to only Airflow delay.")

            else:
                logger.warning(f"Could not resolve pod name for task {task_id}. Falling back to only Airflow delay.")

            total_task_startup = airflow_delay + k8s_startup
            map_index_startups[mi] += total_task_startup
            map_index_pulls[mi] += pull_time

            logger.info(f"Task: {task_id} AF delay: {airflow_delay:.2f}s, K8s Startup: {k8s_startup:.2f}s, Pod Pull: {pull_time:.2f}s")

        if map_index_startups:
            avg_group_startup_time = sum(map_index_startups.values()) / len(map_index_startups)
            avg_pull_time = sum(map_index_pulls.values()) / len(map_index_pulls)
        else:
            avg_group_startup_time = 0.0
            avg_pull_time = 0.0
            logger.warning("No startup data available for this group.")

        return total_wall_duration, avg_group_startup_time, avg_pull_time


    def _resolve_pod_name(self, dag_id: str, dag_run_id: str, task_id: str, map_index: int) -> Optional[str]:
        """Resolve k8s pod name for a given task instance"""
        safe_run_id = parse.quote(dag_run_id)
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json"
        }

        map_index_param = {"map_index": map_index} if map_index >= 0 else {}

        # get pod name from xcom
        try:
            url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns/{safe_run_id}/taskInstances/{task_id}/xcomEntries/pod_name"
            resp = requests.get(url, headers=headers, timeout=5, params=map_index_param)
            if resp.ok:
                pod_name = resp.json().get("value")
                if pod_name:
                    logger.info(f"Resolved pod name for {task_id}: {pod_name}")
                    return pod_name
        except Exception as e:
            logger.warning(f"Failed to resolve pod name from xcom ({e})")

        # rendered fields
        try:
            url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns/{safe_run_id}/taskInstances/{task_id}/renderedFields"
            resp = requests.get(url, headers=headers, timeout=5, params=map_index_param)
            if resp.ok:
                fields = resp.json().get("rendered_fields", {})
                pod_name = fields.get("name") or fields.get("pod_name")
                if pod_name:
                    logger.info(f"Resolved pod name from rendered fields for {task_id}: {pod_name}")
                    return pod_name
        except Exception as e:
            logger.warning(f"Failed to resolve pod name from rendered fields ({e})")


        logger.warning(f"Failed to resolve pod name for {task_id}")
        return None

    def _get_pod_lifecycle_events(self, pod_name: str) -> Dict[str, float]:
        """Query kubectl for pod's lifecycle events"""
        try:
            try:
                config.load_incluster_config()
            except config.config_exception.ConfigException:
                config.load_kube_config()

            v1 = client.CoreV1Api()

            events = v1.list_namespaced_event(namespace=self.namespace, field_selector=f"involvedObject.name={pod_name}")
        except ApiException as e:
            logger.warning(f"Kubernetes API Exception when querying events: {e}")
            return {}
        except Exception as e:
            logger.warning(f"Unexpected error when configuring Kubernetes client: {e}")
            return {}

        lifecycle: Dict[str, float] = {}

        for event in events.items:
            reason = event.reason

            dt = event.first_timestamp or event.event_time
            if not dt:
                continue

            ts = dt.timestamp()

            if reason == "Scheduled":
                lifecycle["scheduled"] = ts

            elif reason == "Pulling":
                if "pulling" not in lifecycle or ts < lifecycle["pulling"]:
                    lifecycle["pulling"] = ts

            elif reason == "Pulled":
                lifecycle["pulled"] = ts

                # try to read pull duration from message
                msg = event.message or ""
                match = re.search(r'in ([\d.]+)(ms|s)', msg)
                if match:
                    try:
                        value = float(match.group(1))
                        unit = match.group(2)
                        # convert to seconds
                        lifecycle["pull_duration"] = value / 1000.0 if unit == "ms" else value
                    except ValueError:
                        pass

            elif reason == "Created":
                lifecycle["created"] = ts

            elif reason == "Started":
                lifecycle["started"] = ts

        if not lifecycle:
            logger.warning("No lifecycle events found for this pod")
        else:
            loggable = {
                k: (datetime.fromtimestamp(v).isoformat() if k != "pull_duration" else f"{v:.3f}s")
                for k, v in sorted(lifecycle.items())
            }
            logger.debug(f"Lifecycle events for '{pod_name}': {loggable}")

        return lifecycle


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