import pytest
from arbo_lib.core.amdahl import AmdahlUtils

def test_calculate_theoretical_time_basic():
    """
    Manual Calculation:
    Startup = 10, Gamma = 1, T_base = 100, p = 0.5, s = 2
    Serial part = 100 * 0.5 = 50
    Parallel part = (100 * 0.5) / 2 = 25
    Total = 10 + 50 + 25 = 85
    :return:
    """
    t = AmdahlUtils.calculate_theoretical_time(c_startup=10, gamma=1, t_base=100, p=0.5, s=2, k=1.0)
    assert t == 85

def test_calculate_current_p():
    """
    Infer 'p' from calculation/result above
    :return:
    """
    p = AmdahlUtils.calculate_current_p(s=2, t_actual=85, c_startup=10, t_base=100, gamma=1, k=1.0)
    assert p == 0.5

def test_circular_consistency():
    """
    if predicted time was with p=0.8, then inferred p should be close to 0.8
    :return:
    """
    original_p = 0.8
    t_pred = AmdahlUtils.calculate_theoretical_time(c_startup=5.0, gamma=1.0, t_base=200.0, p=original_p, s=4, k=1.0)

    inferred_p = AmdahlUtils.calculate_current_p(s=4, t_actual=t_pred, c_startup=5.0, t_base=200.0, gamma=1.0, k=1.0)

    assert inferred_p == pytest.approx(original_p)


def test_edge_case1():
    """
    If s=1, then p cannot be inferred (returns None
    :return:
    """
    p = AmdahlUtils.calculate_current_p(s=1, t_actual=100, c_startup=5.0, t_base=200.0, gamma=1.0, k=1.0)
    assert (p is None)

def test_clamping():
    """
    If execution impossibly fast -> clamp to 0.99
    If execution slower than serial -> clamp to 0.01
    :return:
    """
    p_fast = AmdahlUtils.calculate_current_p(s=10, t_actual=1, c_startup=5.0, t_base=200.0, gamma=1.0, k=1.0)
    assert p_fast == 0.99

    p_slow = AmdahlUtils.calculate_current_p(s=10, t_actual=220, c_startup=5.0, t_base=200.0, gamma=1.0, k=1.0)
    assert p_slow == 0.01

def test_moving_average():
    """
    Test moving average update rule
    :return:
    """
    res = AmdahlUtils.update_moving_average(old_val=1, current_val=0.0, alpha=0.5)
    assert res == 0.5

    res_none = AmdahlUtils.update_moving_average(old_val=0.8, current_val=None, alpha=0.5)
    assert res_none == 0.8

def test_calculate_current_k():
    """
    test power law inference
    :return:
    """
    k = AmdahlUtils.calculate_current_k(s=1, t_actual=400, c_startup=0, t_base=100, gamma=2.0, p=0)
    assert k == pytest.approx(2.0)