import time
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.decorators import task
from kubernetes.client import models as k8s
from datetime import datetime, timedelta

from airflow.utils.trigger_rule import TriggerRule

from arbo_lib.airflow.optimizer import ArboOptimizer

default_args = {
    "owner": 'user',
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

MINIO_ENDPOINT = "minio.minio.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
CHROM_NR = "22"
MINIO_BUCKET = "genome-data"
KEY_INPUT_INDIVIDUAL = "ALL.chr22.80000.vcf.gz"
KEY_INPUT_SIFTING = "ALL.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.sites.annotation.vcf.gz"

TOTAL_ITEMS = 25000

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

    # setup task
    @task()
    def prepare_individual_tasks():
        optimizer = ArboOptimizer()
        start_time = time.time()

        # TODO: figure out way to get gamma (will use 1 for now)
        # TODO: figure out way to get cluster load (will use 0 for now)

        # TODO: try with random values for cluster load
        gamma = 0.5
        configs = optimizer.get_task_configs("genome_individual", gamma=gamma, cluster_load=0.0)
        s_opt = len(configs)

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

        print(f"PLAN: s={s_opt}, chunk_size={chunk_size}")

        return {
            "pod_arguments": pod_argument_list,
            "merge_keys_str": ",".join(merge_keys),
            "s": s_opt,
            "start_time": start_time,
            "gamma": gamma,
            "cluster_load": 10
        }

    plan = prepare_individual_tasks()

    @task
    def extract_pod_args(data: dict):
        return data["pod_arguments"]


    pod_args_clean = extract_pod_args(plan)

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