# ARBO

This project contains a preliminary verion of the ARBO algorithm.

As of now, this only work locally (Postgresql DB in a docker container and Airflow locally (in venv wiht airflow standalone))

## Project Structure
```text
.
├── arbo_lib/                      # core library
│   ├── core/                     
│   │   ├── estimator.py           #    - Main optimization loop (ArboEstimator)
│   │   ├── amdahl.py              #    - Amdahl's Law calculations (p inference)
│   │   └── residual.py            #    - Gaussian Process for noise modeling
│   │
│   ├── db/                      
│   │   ├── store.py               #    - PostgreSQL interaction layer
│   │   └── schema.sql             #    - Database schema definitions
│   │
│   ├── airflow/                  
│   │   ├── optimizer.py           #    - Airflow Operator helper (ArboOptimizer)
│   │   └── dags_utils.py          #    - (Optional) DAG factories
│   │
│   └── config.py                  #    - Global configuration settings
│
├── dags/                         
│   └── genome_dag.py              #    - Example pipeline using Dynamic Task Mapping
│
├── tests/                        
│   ├── core/                      #    - Unit tests for math/logic
│   ├── db/                        #    - Integration tests for database
│   └── conftest.py                #    - Pytest fixtures (DB cleaning)
│
├── report/                        
│   ├── method.tex                 #    - LaTeX source for the thesis/report
│   ├── plots.py                   #    - Scripts to generate performance graphs
│   └── plots/                     #    - Generated images (Pareto frontiers, etc.)
│
├── setup.py                       # makes `arbo_lib` pip-installable for Airflow
│                                 
└── README.md                   
```

This project is structured into 3 main parts:
1. Core library (`arbo_lib`); this contains the main optimization loop and the Amdahl's Law calculations
2. Airflow Operator (`arbo_airflow`); for now this only caóntains a wrapper around the core library for easier usage
3. Databse (`arbo_db`); contains the PostgreSQL schema and interaction logic


## Setup
The `setup.py` file contains the necessary metadata to make the library pip-installable.
The `setup.sh` file contains the commands to export environment variables and start Airflow (only locally so far)

Steps to run the project:
1. Setup Database:
   2. connect pgAdmin4 to the database server (optional, but makes interaction easy)
   3. Naviate to `arbo_lib/db/schema.sql`, copy the content and execute it in the database server
   4. Create a new database `arbo_test` and use the same schema as in `arbo_state` (optinal, for testing purposes)
2. Execute the model
   1. `pip install -e .`this contains the Airflow Operator helper class and DAG factories
   2. `./setup.sh`
   3. Access the Airflow UI at `localhost:8080`


## High Level Abstraction
It requires 3 main components:
1. State Store (DB): stores historical runs, current parameters and baseline metrics
2. Estimator (Python): logic containing Amdahl's Law and GP (proposed algorithm)
3. Interceptor: A hook in DAG to query Estimator before usage and update metrics afterwards

## Open Questions / Things to keep in Mind
- introduce a learning rate for P_obs  --> solved?
- check input  linearity assumptions and maybe change gamma
- cluster load definition might be too naive, should be good for basic version
- include some mechanism to keep the datapoints small/fresh (handle concept drift, is faster (slicing window))  --> currently uses only last 50 executions for prediction
- maybe after each interval run an execution with `s=1` to keep baseline metrics fresh

## Data Model (Step 1)
schema can be found in `arbo_lib/db/schema.sql`

### Testing Strategy
To ensure reliabilty and no loss of data in the main database (`arbo_state`), the project uses an additional database, strictly for testing

1. The test database (`arbo_test`). For the test to run properly, this database needs to exist and have exactly the same schema as the main database (`arbo_state`)
2. The testing framework (`pytest`) is configured to run against the test database (`arbo_test`)
3. Run the tests locally with `pytest -v` or `pytest tests/db/test_arbo.py` to only run the storage tests

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