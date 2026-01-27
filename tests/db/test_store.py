import pytest
from arbo_lib.db.store import ArboState
from arbo_lib.core.exceptions import TaskNotFoundError, TaskAlreadyExistsError

def initialize_task_test(db_clean):
    store = ArboState()
    task_name = "test_task_1"

    store.initialize_task(task_name=task_name, t_base=100.0, base_input_quantity=100)

    model = store.get_task_model(task_name)
    assert model is not None
    assert model["task_name"] == task_name
    assert model["t_base_1"] == 100.0
    assert model["p_obs"] == 1.0
    assert model["sample_count"] == 0
    assert model["base_input_quantity"] == 100

def test_initialize_duplicate_task(db_clean):
    store = ArboState()
    task_name = "duplicate_task"

    store.initialize_task(task_name=task_name, t_base=100.0, base_input_quantity=100)

    with pytest.raises(TaskAlreadyExistsError):
        store.initialize_task(task_name=task_name, t_base=100.0, base_input_quantity=100)

def test_update_and_history_1(db_clean):
    store = ArboState()
    task_name = "test_task_2"

    store.initialize_task(task_name=task_name, t_base=100.0, base_input_quantity=100)

    run_data = {
        "task_name": task_name,
        "s": 4,
        "gamma": 1.2,
        "cluster_load": 12,
        "total_duration": 110,
        "residual": 10,
        "cost_metric": 100
    }

    store.update_model(task_name, new_p=0.8, run_data=run_data)

    updated_model = store.get_task_model(task_name)
    assert updated_model["p_obs"] == 0.8
    assert updated_model["sample_count"] == 1

    history = store.get_history(task_name)
    assert len(history) == 1
    assert history[0]["parallelism"] == 4
    assert history[0]["input_scale_factor"] == 1.2
    assert history[0]["cluster_load"] == 12
    assert history[0]["total_duration"] == 110
    assert history[0]["residual"] == 10
    assert history[0]["cost_metric"] == 100

def test_update_and_history_2(db_clean):
    store = ArboState()
    task_name = "test_task_2"

    store.initialize_task(task_name=task_name, t_base=100.0, alpha=0.5, base_input_quantity=100)

    run_data_1 = {
        "task_name": task_name,
        "s": 4,
        "gamma": 1.2,
        "cluster_load": 12,
        "total_duration": 110,
        "residual": 10,
        "cost_metric": 100,
        "p_snapshot": 0.8
    }

    store.update_model(task_name, new_p=0.8, run_data=run_data_1)

    # check model after 1st run
    model_1 = store.get_task_model(task_name)
    assert model_1["p_obs"] == 0.8
    assert model_1["sample_count"] == 1

    run_data_2 = {
        "task_name": task_name,
        "s": 6,
        "gamma": 1.5,
        "cluster_load": 5,
        "total_duration": 90,
        "residual": 5,
        "cost_metric": 150,
        "p_snapshot": 0.75
    }

    store.update_model(task_name, new_p=0.75, run_data=run_data_2)

    # check model after 2nd run
    model_2 = store.get_task_model(task_name)
    assert model_2["p_obs"] == 0.75
    assert model_2["sample_count"] == 2

    # check history
    history = store.get_history(task_name)
    assert len(history) == 2

    run1 = next(r for r in history if r["parallelism"] == 4)
    assert run1["p_snapshot"] == 0.8
    assert run1["input_scale_factor"] == 1.2

    run2 = next(r for r in history if r["parallelism"] == 6)
    assert run2["p_snapshot"] == 0.75
    assert run2["input_scale_factor"] == 1.5

def test_update_missing_task(db_clean):
    store = ArboState()

    run_data = {
        "task_name": "non_existing_task",
        "s": 4,
        "gamma": 1.2,
        "cluster_load": 12,
        "total_duration": 110,
        "residual": 10,
        "cost_metric": 100
    }

    with pytest.raises(TaskNotFoundError):
        store.update_model("non_existing_task", new_p=0.8, run_data=run_data)