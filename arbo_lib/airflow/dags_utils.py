from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from arbo_lib.airflow.optimizer import ArboOptimizer
import time


def create_arbo_task_group(group_id, task_name, image, cmd, arguments_template, env_vars, dag, total_items, gamma=1.0):
    """
    Creates a TaskGroup that contains the Prepare -> Execute -> Feedback loop.

    :param group_id: Unique ID for the group (e.g. 'individual_stage')
    :param task_name: Name used in Arbo DB (e.g. 'genome_individual')
    :param arguments_template: Function that takes (start, stop) and returns list of args
    """
    from airflow.utils.task_group import TaskGroup

    with TaskGroup(group_id=group_id, dag=dag) as tg:
        @task(task_id="prepare")
        def prepare():
            optimizer = ArboOptimizer()
            start_time = time.time()

            # 1. Ask Arbo
            configs = optimizer.get_task_configs(task_name, gamma=gamma)
            s = len(configs)
            chunk_size = total_items // s

            # 2. Generate Args
            pod_args = []
            for i in range(s):
                start = i * chunk_size + 1
                stop = (i + 1) * chunk_size + 1 if i < s - 1 else total_items + 1

                # Use the template function to generate specific args
                args = arguments_template(start, stop)
                pod_args.append(args)

            return {"s": s, "pod_args": pod_args, "start_time": start_time}

        plan = prepare()

        # Helper to extract list for mapping
        @task(task_id="extract_args")
        def extract(data): return data["pod_args"]

        pod_args = extract(plan)

        # 3. Execute
        workers = KubernetesPodOperator.partial(
            task_id="worker",
            name=f"{task_name}-worker",
            image=image,
            cmds=cmd,
            env_vars=env_vars,
            get_logs=True,
            is_delete_operator_pod=True
        ).expand(
            arguments=pod_args
        )

        # 4. Feedback
        @task(task_id="feedback", trigger_rule="all_success")
        def feedback(setup_data):
            optimizer = ArboOptimizer()
            duration = time.time() - setup_data["start_time"]
            optimizer.report_success(
                task_name=task_name,
                total_duration=duration,
                s=setup_data["s"],
                gamma=gamma,
                cluster_load=0  # Placeholder
            )

        workers >> feedback(plan)

    return tg