import numpy as np
from typing import Optional

from arbo_lib.utils.logger import get_logger

logger = get_logger("arbo.amdahl")


class AmdahlUtils:

    @staticmethod
    def calculate_theoretical_time(c_startup: float, gamma: float, t_base: float, p: float, s: int, k: float) -> float:
        """
        Calculate theoretical execution time based on Amdahl's Law with Input Scaling and Overhead
        :param c_startup: startup time of the task
        :param gamma: scaling factor
        :param t_base: baseline execution time
        :param p: parallelizable part of the task
        :param s: degree of parallelism
        :param k: exponent for the scaling factor
        :return: theoretical execution time
        """

        logger.info(f"calculate_theoretical_time called with c_startup={c_startup:.4f}s (s={s}, gamma={gamma:.2f})")

        if s < 1:
            s = 1

        scaling_factor = gamma ** k
        amdahl_part = (1 - p) * t_base + (p / s) * t_base

        return c_startup + (scaling_factor * amdahl_part)

    @staticmethod
    def calculate_current_p(s: float, t_actual: float, c_startup: float, t_base: float, gamma: float, k: float) -> \
    Optional[float]:
        """
        Infer observed 'p' from a single execution in comparison to baseline time.
        :param s: degree of parallelism
        :param t_actual: actual execution time
        :param c_startup: startup time of the task
        :param t_base: baseline execution time
        :param gamma: scaling factor
        :param k: exponent for the scaling factor
        :return: inferred p or None if not possible
        """

        logger.info(f"Inferring 'p' using c_startup={c_startup:.4f}s (t_actual={t_actual:.2f}s)")

        if s <= 1 or t_base <= 0:
            return None

        pure_computation_time = max(0.0, t_actual - c_startup)

        expected_scale = gamma ** k
        if expected_scale <= 0:
            return None

        normalized_time = pure_computation_time / (expected_scale * t_base)

        p_calc = (s / (s - 1)) * (1 - normalized_time)

        return max(0.01, min(0.99, p_calc))  # clamp to [0.01, 0.99]

    @staticmethod
    def calculate_current_k(s: int, t_actual: float, c_startup: float, t_base: float, gamma: float, p: float) -> \
    Optional[float]:
        """
        Infer observed 'k' from a single execution
        :param s: degree of parallelism
        :param t_actual: actual execution time
        :param c_startup: startup time of the task
        :param t_base: baseline execution time
        :param gamma: scaling factor
        :param p: parallelizable part of the task
        :return:
        """

        logger.info(f"Inferring 'k' using c_startup={c_startup:.4f}s (t_actual={t_actual:.2f}s)")

        if 0.99 <= gamma <= 1.01:  # input scale has not changed significantly
            return None

        pure_time = max(1e-3, t_actual - c_startup)

        theoretical_base_at_s = ((1 - p) * t_base) + ((p / s) * t_base)

        if theoretical_base_at_s <= 0:
            return None

        ratio = pure_time / theoretical_base_at_s

        if ratio <= 0:
            return None

        try:
            k_calc = np.log(ratio) / np.log(gamma)
            return max(0.5, min(3.0, k_calc))  # clamp to square root and cubic complexity
        except ZeroDivisionError:
            return None

    @staticmethod
    def update_moving_average(old_val: float, current_val: Optional[float], alpha: float):
        """
        Update moving average with the new value
        :param old_val: old value
        :param current_val: new value
        :param alpha: learning rate
        :return: updated average
        """
        if current_val is None:
            return old_val

        return alpha * old_val + (1 - alpha) * current_val

