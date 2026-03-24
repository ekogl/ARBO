import boto3
from botocore.client import Config as botoConfig
from typing import Optional
from arbo_lib.utils.logger import get_logger

logger = get_logger("arbo.storage")

class MinioClient:
    @staticmethod
    def get_filesize(endpoint_url: str, access_key: str, secret_key: str, bucket_name: str, file_key: str) -> Optional[float]:
        """Queries MinIO for file size in bytes."""
        try:
            s3 = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=botoConfig(signature_version='s3v4')
            )
            obj = s3.head_object(Bucket=bucket_name, Key=file_key)
            size = obj["ContentLength"]
            logger.info(f"MinIO Success: {file_key} is {size} bytes")
            return float(size)
        except Exception as e:
            logger.warning(f"MinIO Query Failed ({e})")
            return None

    @staticmethod
    def get_directory_size(endpoint_url: str, access_key: str, secret_key: str, bucket_name: str, prefix: str) -> Optional[float]:
        """Queries MinIO for total size of a directory prefix."""
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
            logger.info(f"MinIO Success: {prefix} is {total_size} bytes")
            return float(total_size)
        except Exception as e:
            logger.warning(f"MinIO Directory Query Failed ({e})")
            return None
