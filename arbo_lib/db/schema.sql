CREATE TABLE task_models (
    task_name VARCHAR(255) PRIMARY KEY,
    t_base_1 FLOAT,             -- Baseline time at s=1 (seconds)
    p_obs FLOAT,                -- Current dynamic parallelizable portion
    c_startup FLOAT,            -- Fixed overhead (e.g., pod spin-up)
    alpha FLOAT DEFAULT 0.3,    -- Learning rate for p_obs
    last_updated TIMESTAMP,
    sample_count INT DEFAULT 0  -- optional?
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