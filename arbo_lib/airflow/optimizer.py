from typing import List, Dict
import psutil
import boto3
from botocore.client import Config
from typing import Optional

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

    @staticmethod
    def get_filesize(endpoint_url: str, access_key: str, secret_key: str, bucket_name: str, file_key: str) -> Optional[float]:
        """
        Queries MinIO for file size
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

    # TODO: function to get execution time of task

    @staticmethod
    def get_virtual_memory():
        """
        Gets virtual memory usage, returns memory usage
        NOTE: This should not be used in production settings
        """
        mem = psutil.virtual_memory()
        return mem.percent / 100