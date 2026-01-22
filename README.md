# ARBO

## High Level Abstraction
It requires 3 main components:
1. State Store (DB): stores historical runs, current parameters and baseline metrics
2. Estimator (Python): logic containing Amdahl's Law and GP (proposed algorithm)
3. Interceptor: A hook in DAG to query Estimator before usage and update metrics afterwards


## Data Model (Step 1)
```sql
CREATE TABLE task_models (
    task_name VARCHAR(255) PRIMARY KEY,
    t_base_1 FLOAT,            -- Baseline time at s=1 (seconds)
    p_obs FLOAT,               -- Current dynamic parallelizable portion
    c_startup FLOAT,           -- Fixed overhead (e.g., pod spin-up)
    alpha FLOAT DEFAULT 0.3,   -- Learning rate for p_obs
    last_updated TIMESTAMP
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