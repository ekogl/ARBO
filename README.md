# ARBO

## High Level Abstraction
It requires 3 main components:
1. State Store (DB): stores historical runs, current parameters and baseline metrics
2. Estimator (Python): logic containing Amdahl's Law and GP (proposed algorithm)
3. Interceptor: A hook in DAG to query Estimator before usage and update metrics afterwards

## Open Questions / Things to keep in Mind
- introduce a learning rate for P_obs
- check input  linearity assumptions and maybe change gamma
- cluster load definition might be too naive, should be good for basic version
- include some mechanism to keep the datapoints small/fresh (handle concept drift, is faster (slicing window))

## Data Model (Step 1)
```sql
CREATE TABLE task_models (
    task_name VARCHAR(255) PRIMARY KEY,
    t_base_1 FLOAT,            -- Baseline time at s=1 (seconds)
    p_obs FLOAT,               -- Current dynamic parallelizable portion
    c_startup FLOAT,           -- Fixed overhead (e.g., pod spin-up)
    alpha FLOAT DEFAULT 0.3,   -- Learning rate for p_obs
    last_updated TIMESTAMP
    -- maybe add sample count?
    sample_count INT DEFAULT 0
);

CREATE TABLE execution_history (
    id SERIAL PRIMARY KEY,
    task_name VARCHAR(255),
    parallelism INT,           -- s
    input_scale_factor FLOAT,  -- gamma
    cluster_load INT,          -- L_cluster
    execution_time FLOAT,      -- Actual T
    residual FLOAT,            -- T_actual - T_amdahl
    FOREIGN KEY (task_name) REFERENCES task_models(task_name)
);
```

## Estimator (Step 2):
implement model, dynamic update loop, optimization loop, integration to cluster to fetch metrics ...


## Interceptor (Step 3):
figure out a smart way to integrate model into DAG

### Option A: (At parse time)
maybe use sth like (would run on the scheduler -> bad for performance):
```python
default_args = {
    "owner": 'user',
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # Add callback to capture execution time automatically
    "on_success_callback": ArboOptimizer().feedback_callback 
}

# initialize Optimizer before DAG definition
optimizer = ArboOptimizer()

# some DAG code
INDIVIDUAL_WORKERS = optimizer.get_optimal_parallelism("individual", default_val=5)
FREQUENCY_ALL_WORKERS = optimizer.get_optimal_parallelism("frequency_ALL", default_val=1)
FREQUENCY_EUR_WORKERS = optimizer.get_optimal_parallelism("frequency_EUR", default_val=1)

# keep structure with for-loops
```
`get_optimal_parallelism`: should query DB for metrics, run optimization loop and return s
`ArboOptimizer`: should contain a callback function, that is exeuted when task is finished to update DB with parameters 

since Arbo is initialized in top-level code, this will run every time DAG gets parsed, might need to implement some sort of caching to avoid constant DB queries which would kill the scheduler

### Option B: (JIT with Dynamic Task Mapping)
This runs optimization as a standalone task immediately before the heavy workload (on the worker; scheduler is safe; no pod-spin-up time). This is safer for the scheduler and provides fresher metrics (e.g., accurate Cluster Load at execution time).

It uses `Airflow's Dynamic Task Mapping` (.expand) instead of Python `for` loops.

```python
# 1. Define the Optimization Task (Runs on Worker, safe for heavy logic)
@task
def get_task_configs(task_name, default_s):
    # Initialize logic here (Worker side)
    optimizer = ArboOptimizer() 
    
    # Run heavy optimization loop (Query DB + Prometheus)
    s_opt = optimizer.get_optimal_parallelism(task_name, default_s)
    
    # Return a LIST of arguments for downstream tasks
    # This list length determines the number of parallel pods
    configs = []
    for i in range(s_opt):
        configs.append([
            "--chunk_id", str(i), 
            "--chunks", str(s_opt), 
            "--POP", "ALL"
        ])
    return configs

# 2. In the DAG definition
# Get the config list (Lazy execution)
dynamic_args = get_task_configs("frequency_ALL", 4)

# 3. Map the heavy task over the list
KubernetesPodOperator.partial(
    task_id="frequency_calc_plot",
    name="frequency-calc-plot",
    image="kogsi/genome_dag:frequency_par2",
    cmds=["python3", "frequency_par2.py"],
    # ... constant args ...
).expand(
    # Dynamic Mapping: Creates N pods based on list length
    arguments=dynamic_args 
)
```

Adds an additional start-up cost for pods, but safer for scheduler and scales better in the cluster

Requires restructuring the existing DAGs (should be feasible), check in detail how .expand() works (task_id and map indexes)

## Way to integrate SLOs
### Execution time
- let user define max time it can take
- in the optimization loop discard all the configurations where time is too high
- then use cheapest one/ or other metric

### Cost
- calculate cost based on s * predicted duration * cost per second (for example)
- get predictions and cut of all that are too high
- then pick fastest one