import time
import boto3
from airflow.cli.commands.task_command import task_state
from botocore.client import Config
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.decorators import task, task_group
from kubernetes.client import models as k8s
from datetime import datetime, timedelta

from airflow.utils.trigger_rule import TriggerRule

from arbo_lib.airflow.optimizer import ArboOptimizer
from arbo_lib.utils.logger import get_logger

logger = get_logger("arbo.iisas_image_training")

default_args = {
    "owner": "user",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

MINIO_ENDPOINT = "minio.minio.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "image-classification-data"

NAMESPACE = "default"

minio_env_dict = {
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "MINIO_SECURE": "false"
}

NUM_OF_PICTURES = 8

with DAG(
    dag_id="arbo_iisas_image_training",
    default_args=default_args,
    description="IISAS Image Classification Training Pipeline",
    schedule=None,
    catchup=False,
    tags=["iisas", "kubernetes", "minio"],
    max_active_tasks=20,
) as dag:

    # setup task
    @task
    def prepare_pipeline_configs():
        import psutil

        optimizer = ArboOptimizer()
        start_time = time.time()

        # TODO: change later
        mem = psutil.virtual_memory()
        cluster_load = mem.percent / 100

        try:
            s3 = boto3.client(
                "s3",
                endpoint_url=f"http://localhost:9000",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
                config=Config(signature_version="s3v4")
            )
            paginator = s3.get_paginator("list_objects_v2")

            total_size = 0

            for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix="training/input"):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        total_size += obj["Size"]
            input_quantity = total_size
            logger.info(f"MinIO Query Successful: Total Input Size is {total_size} bytes")
        except Exception as e:
            logger.warning(f"MinIO Query Failed ({e}). Falling back to static TOTAL_ITEMS.")
            input_quantity = NUM_OF_PICTURES * 1024 * 1024

        configs = optimizer.get_task_configs("iisas_image_training", input_quantity=input_quantity, cluster_load=cluster_load)
        s_opt = len(configs)

        calculated_gamma = configs[0]["gamma"]

        logger.info(f"Configuration received: s={s_opt}, gamma={calculated_gamma}")

        configurations = []
        for i in range(s_opt):
            config = {
                "chunk_id": str(i),
                "offset_args": [
                    "--input_image_path", "training/input",
                    "--output_image_path", f"training/offsetted/{i}",
                    "--dx", "0", "--dy", "0",
                    "--bucket_name", MINIO_BUCKET,
                    "--chunk_id", str(i),
                    "--num_tasks", str(s_opt),
                ],
                "crop_args": [
                    "--input_image_path", f"training/offsetted/{i}",
                    "--output_image_path", f"training/cropped/{i}",
                    "--left", "20", "--top", "20",
                    "--right", "330", "--bottom", "330",
                    "--bucket_name", MINIO_BUCKET,
                    "--chunk_id", "0", "--num_tasks", "1",
                ],
                "enhance_brightness_args": [
                    "--input_image_path", f"training/cropped/{i}",
                    "--output_image_path", f"training/enhanced_brightness/{i}",
                    "--factor", str(1.2),
                    "--bucket_name", MINIO_BUCKET,
                    "--chunk_id", "0",
                    "--num_tasks", "1",
                ],
                "enhance_contrast_args": [
                    "--input_image_path", f"training/enhanced_brightness/{i}",
                    "--output_image_path", f"training/enhanced_contrast/{i}",
                    "--factor", str(1.2),
                    "--bucket_name", MINIO_BUCKET,
                    "--chunk_id", "0",
                    "--num_tasks", "1",
                ],
                "rotate_args": [
                    "--input_image_path", f"training/enhanced_contrast/{i}",
                    "--output_image_path", f"training/rotated/{i}",
                    "--rotation", " ".join(["0", "90", "180", "270"]),
                    "--bucket_name", MINIO_BUCKET,
                    "--chunk_id", "0",
                    "--num_tasks", "1",
                ],
                "grayscale_args": [
                    "--input_image_path", f"training/rotated/{i}",
                    "--output_image_path", f"training/grayscaled",
                    "--bucket_name", MINIO_BUCKET,
                    "--chunk_id", "0",
                    "--num_tasks", "1",
                ]
            }
            configurations.append(config)

        return {
            "configurations": configurations,
            "metadata": {
                "start_time": start_time,
                "s": s_opt,
                "gamma": calculated_gamma,
                "cluster_load": cluster_load
            }
        }

    setup_data = prepare_pipeline_configs()

    @task
    def extract_configs(data: dict):
        return data["configurations"]

    @task
    def extract_metadata(data: dict):
        return data["metadata"]

    pod_config_list = extract_configs(setup_data)
    pipeline_metadata = extract_metadata(setup_data)

    @task_group(group_id="preprocessing_pipeline")
    def image_pipeline_group(config: dict):

        @task
        def get_args_by_key(conf: dict, key: str):
            return conf[key]

        # Use the helper to resolve arguments
        offset_args_list = get_args_by_key(config, "offset_args")
        crop_args_list = get_args_by_key(config, "crop_args")
        brightness_args_list = get_args_by_key(config, "enhance_brightness_args")
        contrast_args_list = get_args_by_key(config, "enhance_contrast_args")
        rotate_args_list = get_args_by_key(config, "rotate_args")
        grayscale_args_list = get_args_by_key(config, "grayscale_args")

        offset = KubernetesPodOperator(
            task_id="offset",
            name="offset-task",
            namespace=NAMESPACE,
            image="kogsi/image_classification:offset",
            arguments=offset_args_list,  # Inject dynamic args
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
        )

        crop = KubernetesPodOperator(
            task_id="crop",
            name="crop-task",
            namespace=NAMESPACE,
            image="kogsi/image_classification:crop",
            arguments=crop_args_list,
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
        )

        enhance_brightness = KubernetesPodOperator(
            task_id="enhance_brightness",
            name="enhance_brightness-task",
            namespace=NAMESPACE,
            image="kogsi/image_classification:enhance-brightness",
            arguments=brightness_args_list,
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
        )

        enhance_contrast = KubernetesPodOperator(
            task_id="enhance_contrast",
            name="enhance_contrast-task",
            namespace=NAMESPACE,
            image="kogsi/image_classification:enhance-contrast",
            arguments=contrast_args_list,
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
        )

        rotate = KubernetesPodOperator(
            task_id="rotate",
            name="rotate-task",
            namespace=NAMESPACE,
            image="kogsi/image_classification:rotate",
            arguments=rotate_args_list,
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
        )

        grayscale = KubernetesPodOperator(
            task_id="grayscale",
            name="grayscale-task",
            namespace=NAMESPACE,
            image="kogsi/image_classification:to-grayscale",
            arguments=grayscale_args_list,
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
        )

        offset >> crop >> enhance_brightness >> enhance_contrast >> rotate >> grayscale

    # classification_inference = KubernetesPodOperator(
    #     task_id="classification_inference_task_training",
    #     name="classification_inference_task_training",
    #     namespace=NAMESPACE,
    #     image="kogsi/image_classification:classification-train-tf2",
    #     arguments=[
    #         "--train_data_path", "training/grayscaled",
    #         "--output_artifact_path", "models/",
    #         "--bucket_name", MINIO_BUCKET,
    #         "--validation_split", "0.2",
    #         # "--validation_data_path", "training/validation",
    #         "--epochs", "5",
    #         "--batch_size", "32",
    #         "--early_stop_patience", "5",
    #         "--dropout_rate", "0.2",
    #         "--image_size", "256 256",
    #         "--num_layers", "3",
    #         "--filters_per_layer", "64 64 64",
    #         "--kernel_sizes", "3 3 3",
    #         "--workers", "4",
    #     ],
    #     env_vars=minio_env_dict,
    #     get_logs=True,
    #     is_delete_operator_pod=True,
    #     image_pull_policy="IfNotPresent",
    # )

    sleep_task = KubernetesPodOperator(
        task_id="sleep_10s",
        name="sleep-task",
        namespace=NAMESPACE,
        image="alpine:latest",
        cmds=["/bin/sh", "-c"],
        arguments=["echo 'Sleeping now...'; sleep 10; echo 'Awake!'"],
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="IfNotPresent",
    )

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def report_feedback(metadata: dict):
        optimizer = ArboOptimizer()

        # TODO: get execution time form prometheus

        end_time = time.time()
        duration = end_time - metadata["start_time"]

        optimizer.report_success(
            task_name="iisas_image_training",
            total_duration=duration,
            s=metadata["s"],
            gamma=metadata["gamma"],
            cluster_load=metadata["cluster_load"]
        )

    pipeline_configs = prepare_pipeline_configs()

    pipeline_instances = image_pipeline_group.expand(config=pod_config_list)

    # pipeline_instances >> classification_inference
    pipeline_instances >> sleep_task
    pipeline_instances >> report_feedback(pipeline_metadata)