import pytest
from arbo_lib.core.estimator import ArboEstimator

def test_cold_start(db_clean):
    estimator = ArboEstimator()
    task_name = "test_cold_start"

    # 'prediction' for s should be 1
    input_quantity = 100
    s_opt, gamma = estimator.predict(task_name=task_name, cluster_load=0, input_quantity=input_quantity)
    assert s_opt == 1
    assert gamma == 1.0

    # simulate a runtime of 100 seconds
    estimator.feedback(task_name=task_name, s=1, gamma=1.0, cluster_load=0, t_actual=100)

    model = estimator.store.get_task_model(task_name)
    assert model is not None
    assert model["t_base_1"] == 100.0
    assert model["p_obs"] == 1.0
    assert model["sample_count"] == 1
    assert model["base_input_quantity"] == input_quantity

def test_optimization_logic(db_clean):
    estimator = ArboEstimator()
    task_name = "test_optimization_logic"

    estimator.store.initialize_task(task_name=task_name, t_base=100.0, base_input_quantity=100, p=0.9, c_startup=6)

    s_opt, gamma = estimator.predict(task_name=task_name, cluster_load=0, input_quantity=200)
    assert s_opt > 1
    assert gamma == 2


def test_moving_average_logic(db_clean):
    """
    Verifies that p_obs updates correctly using the EMA formula:
    New_P = Alpha * Old_P + (1 - Alpha) * Observed_P
    """
    estimator = ArboEstimator()
    task_name = "test_ema"
    base_input_quantity = 100

    # SETUP:
    # Old P = 0.5
    # Alpha = 0.5
    estimator.store.initialize_task(task_name=task_name, t_base=100.0, p=0.5, c_startup=0.0, alpha=0.5, base_input_quantity=base_input_quantity)

    # manually set sample count to 1
    with estimator.store._get_cursor() as cur:
        cur.execute("UPDATE task_models SET sample_count = 1 WHERE task_name = %s", (task_name,))

    # EXECUTION SCENARIO:
    # Run with s=2.
    # We want Observed P to be 0.8.
    # Formula: T = T_base * ( (1-p) + p/s )
    # T = 100 * (0.2 + 0.4) = 60s
    t_actual = 60.0

    estimator.feedback(task_name=task_name, s=2, gamma=1.0, cluster_load=0, t_actual=t_actual)

    # VERIFICATION:
    # Observed P should be 0.8 (derived from 60s runtime).
    # New P = 0.5 * 0.5 + 0.5 * 0.8
    #       = 0.25 + 0.4
    #       = 0.65

    # check updated model
    model = estimator.store.get_task_model(task_name)
    assert model["p_obs"] == pytest.approx(0.65, abs=0.001)
    assert model["sample_count"] == 2  # since it was incremented by 1

    # check history snapshot
    history = estimator.store.get_history(task_name)
    assert len(history) == 1
    assert history[0]["p_snapshot"] == pytest.approx(0.65, abs=0.001)