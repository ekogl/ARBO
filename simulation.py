import time
import random
import uuid
import numpy as np
from arbo_lib.core.estimator import ArboEstimator
from arbo_lib.core.amdahl import AmdahlUtils

# --- CONFIGURATION: The "Hidden Truth" of our simulated cluster ---
TRUE_T_BASE = 200.0  # Serial time (seconds)
TRUE_P = 0.85  # Real parallelizable portion (85%)
TRUE_C_STARTUP = 10.0  # Kubernetes pod spin-up overhead
NOISE_LEVEL = 0.05  # 5% random variation in execution time


def simulate_cluster_execution(s: int, gamma: float, load: float) -> float:
    """
    Simulates the actual execution time on a cluster.
    This represents the 'Real World' that the Estimator tries to model.
    """
    # 1. Calculate perfect Amdahl time
    # T(s) = C_start + Gamma * ( (1-p)*T_base + (p/s)*T_base )
    serial_part = TRUE_T_BASE * (1 - TRUE_P)
    parallel_part = (TRUE_T_BASE * TRUE_P) / s

    t_theoretical = TRUE_C_STARTUP + gamma * (serial_part + parallel_part)

    # 2. Add Cluster Load Penalty (Simple model: 1% slowdown per load unit)
    load_penalty = t_theoretical * (load * 0.01)

    # 3. Add Random Noise (e.g., network jitter)
    noise = t_theoretical * random.uniform(-NOISE_LEVEL, NOISE_LEVEL)

    return t_theoretical + load_penalty + noise


def print_step(step, msg):
    print(f"\n{'=' * 60}")
    print(f" STEP {step}: {msg}")
    print(f"{'=' * 60}")


def run_simulation():
    # 1. Setup
    estimator = ArboEstimator()
    task_name = f"sim_task_{str(uuid.uuid4())[:8]}"  # Unique name to ensure clean state

    print(f" STARTING SIMULATION for Task: '{task_name}'")
    print(f" HIDDEN TRUTH: T_base={TRUE_T_BASE}s, p={TRUE_P}, Startup={TRUE_C_STARTUP}s")

    # --- SIMULATION LOOP ---
    for i in range(1, 6):
        run_id = i

        # Vary input size (gamma) and cluster load slightly
        gamma = 1.0 if i < 3 else 1.2  # Increase input size after run 2
        cluster_load = random.randint(0, 20)

        # --- A. PREDICT ---
        print_step(f"{run_id}.A", "ASKING ESTIMATOR")
        print(f"   Context: Input Scale (gamma)={gamma}, Cluster Load={cluster_load}%")

        s_opt = estimator.predict(task_name, gamma, cluster_load)
        print(f"    Estimator Decision: Run with s = {s_opt}")

        # --- B. EXECUTE (Simulated) ---
        print_step(f"{run_id}.B", "RUNNING ON CLUSTER")
        t_actual = simulate_cluster_execution(s_opt, gamma, cluster_load)
        print(f"   ️  Actual Duration: {t_actual:.2f}s")

        # --- C. FEEDBACK (Learn) ---
        print_step(f"{run_id}.C", "LEARNING")
        estimator.feedback(task_name, s_opt, gamma, cluster_load, t_actual)

        # --- D. INSPECT INTERNAL STATE ---
        model = estimator.store.get_task_model(task_name)
        if model:
            print(f"    Updated Belief: p_obs = {model['p_obs']:.4f}, Sample Count = {model['sample_count']}")
            if model['sample_count'] > 1:
                # Calculate error
                p_error = abs(model['p_obs'] - TRUE_P)
                print(f"    Error from Truth: {p_error:.4f}")

        time.sleep(0.5)  # Pause for readability

    print("\n✅ SIMULATION COMPLETE")


if __name__ == "__main__":
    run_simulation()