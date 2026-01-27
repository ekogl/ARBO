# example usage

```python
from arbo_lib.airflow.dag_utils import create_arbo_task_group

with DAG(...) as dag:

    # Define how to build args for the "Individual" stage
    def individual_args_builder(start, stop):
        return [
            "--key_input", "ALL.chr22...", 
            "--counter", str(start), 
            "--stop", str(stop)
        ]

    # Create the whole stage in one line
    individual_stage = create_arbo_task_group(
        group_id="individual_stage",
        task_name="genome_individual",
        image="kogsi/genome_dag:individual",
        cmd=["python3", "individual.py"],
        arguments_template=individual_args_builder,
        env_vars=minio_env_vars,
        dag=dag,
        total_items=50000
    )

    # Next stage...
    sifting_stage = ...

    # Dependencies
    individual_stage >> sifting_stage
```