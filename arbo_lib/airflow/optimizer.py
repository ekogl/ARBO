from typing import List, Dict
import psutil
import boto3
from botocore.client import Config
from typing import Optional
import requests

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
            self, task_name: str, total_duration: float, s: int, gamma: float, cluster_load: float,
            predicted_amdahl: float, predicted_residual: float
    ) -> None:
        """
        callback after the parallel stage is done; feeds actual execution time into the DB
        :param task_name: Unique identifier for task
        :param total_duration: actual execution time of the task
        :param s: degree of parallelism (number of workers)
        :param gamma: input scaling factor
        :param cluster_load: metric representing cluster load
        :param predicted_amdahl: predicted execution time based on Amdahl's Law
        :param predicted_residual: predicted residual (overhead) by the GP
        :return: None
        """
        logger.info(f"Feedback '{task_name}': Actual={total_duration:.2f}s vs Pred={predicted_amdahl + predicted_residual:.2f}s")

        self.estimator.feedback(task_name, s, gamma, cluster_load, total_duration, predicted_amdahl, predicted_residual)

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
                config=Config(signature_version='s3v4')
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
                config=Config(signature_version='s3v4')
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



    # TODO: function to get cluster load
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
            logger.warning(f"Prometheus Query Failed ({e}). Returning 0.0")

        # TODO: properly handle failure
        return self.get_virtual_memory()


    # TODO: function to get execution time of task

    @staticmethod
    def get_virtual_memory() -> float:
        """
        Gets virtual memory usage, returns memory usage
        NOTE: This should not be used in production settings

        :return: Memory usage as a normalized float (0.0 to 1.0
        """
        mem = psutil.virtual_memory()
        return mem.percent / 100