import time
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.decorators import task
from kubernetes.client import models as k8s
from datetime import datetime, timedelta

from airflow.utils.trigger_rule import TriggerRule

from arbo_lib.airflow.optimizer import ArboOptimizer
from arbo_lib.utils.logger import get_logger

logger = get_logger("arbo.genome_dag")

default_args = {
    "owner": "user",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

TOTAL_ITEMS = 80000

MINIO_ENDPOINT = "minio.minio.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
CHROM_NR = "22"
MINIO_BUCKET = "genome-data"
KEY_INPUT_INDIVIDUAL = f"ALL.chr22.{TOTAL_ITEMS}.vcf.gz"
KEY_INPUT_SIFTING = "ALL.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.sites.annotation.vcf.gz"

NAMESPACE = "default"

minio_env_vars = [
    k8s.V1EnvVar(name="MINIO_ENDPOINT", value=MINIO_ENDPOINT),
    k8s.V1EnvVar(name="MINIO_ACCESS_KEY", value=MINIO_ACCESS_KEY),
    k8s.V1EnvVar(name="MINIO_SECRET_KEY", value=MINIO_SECRET_KEY),
    k8s.V1EnvVar(name="MINIO_SECURE", value="false"),
]

with DAG(
        dag_id='arbo_genome',
        default_args=default_args,
        description='Genome processing pipeline using KubernetesPodOperator',
        schedule=None,
        catchup=False,
        tags=['genome', 'kubernetes', 'minio'],
        max_active_tasks=20,
) as dag:

    populations = ["EUR", "AFR", "EAS", "ALL", "GBR", "SAS", "AMR"]

    # setup task
    @task()
    def prepare_individual_tasks():
        optimizer = ArboOptimizer()

        # TODO: figure out way to get cluster load (will use virtual memory for now)
        cluster_load = optimizer.get_virtual_memory()

        logger.info(f"Local Simulation: Cluster Load set to {cluster_load}")

        input_quantity = optimizer.get_filesize(
            endpoint_url="http://localhost:9000",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            bucket_name=MINIO_BUCKET,
            file_key=f"input/{KEY_INPUT_INDIVIDUAL}"
        )

        if not input_quantity:
            logger.info("Falling back to default (= TOTAL_ITEMS)")
            input_quantity = TOTAL_ITEMS

        configs = optimizer.get_task_configs("genome_individual", input_quantity=input_quantity, cluster_load=cluster_load)
        s_opt = len(configs)

        calculated_gamma = configs[0]["gamma"]

        # TODO: change later
        start_time = time.time()

        chunk_size = TOTAL_ITEMS // s_opt

        # generate arguments for each pod
        pod_argument_list = []
        merge_keys = []

        for i in range (s_opt):
            counter = i * chunk_size + 1
            if i == s_opt - 1:
                stop = TOTAL_ITEMS + 1
            else:
                stop = (i + 1) * chunk_size + 1

            args = [
                "--key_input", KEY_INPUT_INDIVIDUAL,
                "--counter", str(counter),
                "--stop", str(stop),
                "--chromNr", CHROM_NR,
                "--bucket_name", MINIO_BUCKET
            ]
            pod_argument_list.append(args)

            # prepare filename key for downstream tasks
            file_key = f'chr22n-{counter}-{stop}.tar.gz'
            merge_keys.append(file_key)

        logger.info(f"PLAN: s={s_opt}, chunk_size={chunk_size}")

        return {
            "pod_arguments": pod_argument_list,
            "merge_keys_str": ",".join(merge_keys),
            "s": s_opt,
            "start_time": start_time,
            "gamma": calculated_gamma,
            "cluster_load": cluster_load
        }

    plan = prepare_individual_tasks()

    @task
    def prepare_frequency_tasks():
        # TODO: implement logic for creating list of list for expand
        pass


    @task
    def extract_pod_args(data: dict):
        return data["pod_arguments"]

    @task
    def extract_merge_keys(data: dict):
        return data["merge_keys_str"]

    @task
    def mutations_overlap_data(pops: list):
        data = []
        for pop in pops:
            data.append([
                "--chromNr", CHROM_NR,
                "--POP", pop,
                "bucket_name", MINIO_BUCKET,
            ])
        return data

    pod_args_clean = extract_pod_args(plan)
    merge_keys = extract_merge_keys(plan)
    mutations_data = mutations_overlap_data(populations)

    # real individual task via dynamic mapping
    individual_tasks = KubernetesPodOperator.partial(
        task_id="individual_worker",
        name="individual-worker",
        namespace=NAMESPACE,
        image="kogsi/genome_dag:individual",
        cmds=["python3", "individual.py"],
        # Arguments are injected dynamically via .expand()
        env_vars=minio_env_vars,
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="IfNotPresent",
        execution_timeout=timedelta(hours=1),
    ).expand(
        arguments=pod_args_clean
    )

    # Sifting task
    siftting_task = KubernetesPodOperator(
        task_id="sifting",
        name="sifting",
        namespace=NAMESPACE,
        image="kogsi/genome_dag:sifting",
        cmds=["python3", "sifting.py"],
        arguments=[
            "--key_input", KEY_INPUT_SIFTING,
            "--bucket_name", MINIO_BUCKET,
            "--bucket_name", MINIO_BUCKET
        ],
        env_vars=minio_env_vars,
        get_logs=True,
        image_pull_policy="IfNotPresent",
        is_delete_operator_pod=True,
    )

    # Individuals Merge task
    individuals_merge_task = KubernetesPodOperator(
        task_id="individuals_merge",
        name="individuals_merge",
        namespace=NAMESPACE,
        image="kogsi/genome_dag:individuals-merge",
        cmds=["python3", "individuals-merge.py"],
        arguments=[
            "--chromNr", CHROM_NR,
            "--keys", merge_keys,
            "--bucket_name", MINIO_BUCKET
        ],
        env_vars=minio_env_vars,
        get_logs=True,
        image_pull_policy="IfNotPresent",
        is_delete_operator_pod=True,
    )

    mutations_tasks = KubernetesPodOperator.partial(
        task_id="mutations_overlap",
        name="mutations-overlap",
        namespace=NAMESPACE,
        image="kogsi/genome_dag:mutations-overlap",
        cmds=["python3", "mutations-overlap.py"],
        env_vars=minio_env_vars,
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="IfNotPresent",
    ).expand(
        arguments=mutations_data
    )

    # feedback task
    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def report_feedback(setup_data: dict):
        optimizer = ArboOptimizer()

        # TODO: get execution time form prometheus

        end_time = time.time()
        duration = end_time - setup_data["start_time"]

        optimizer.report_success(
            task_name="genome_individual",
            total_duration=duration,
            s=setup_data["s"],
            gamma=setup_data["gamma"],
            cluster_load=setup_data["cluster_load"]
        )

    individual_tasks >> report_feedback(plan)

    individual_tasks >> individuals_merge_task >> mutations_tasks
    siftting_task >> mutations_tasks