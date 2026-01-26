CREATE TABLE task_models (
    task_name VARCHAR(255) PRIMARY KEY,
    t_base_1 FLOAT,            -- Baseline time at s=1 (seconds)
    p_obs FLOAT,               -- Current dynamic parallelizable portion
    c_startup FLOAT,           -- Fixed overhead (e.g., pod spin-up)
    alpha FLOAT DEFAULT 0.3,   -- Learning rate for p_obs
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sample_count INT DEFAULT 1
);

CREATE TABLE execution_history (
    id SERIAL PRIMARY KEY,
    task_name VARCHAR(255) REFERENCES task_models(task_name),
    parallelism INT NOT NULL ,           -- s
    input_scale_factor FLOAT NOT NULL,  -- gamma
    cluster_load INT NOT NULL,          -- L_cluster
    total_duration FLOAT NOT NULL,      -- Actual T (Wall Clock Time)
    residual FLOAT NOT NULL,            -- T_actual - T_amdahl
    cost_metric FLOAT,                 -- Cost of run
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster lookups by task name
CREATE INDEX idx_history_task ON execution_history(task_name);