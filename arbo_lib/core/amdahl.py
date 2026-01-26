# TODO: implement logic from method.pdf
from typing import Optional


class AmdahlUtils:

    @staticmethod
    def calculate_theoretical_time(c_startup: float, gamma: float, t_base: float, p: float, s: int) -> float:
        """
        Calculate theoretical execution time based on Amdahl's Law with Input Scaling and Overhead
        :return:
        """
        if s < 1:
            s = 1

        return c_startup + gamma * ((1 - p) * t_base + (p/s) * t_base)

    @staticmethod
    def calculate_current_p(s: float, t_actual: float, c_startup: float, t_base: float, gamma: float) -> Optional[float]:
        """
        Infer observed 'p' from a single execution in comparison to baseline time.
        returns None if s=1
        :return:
        """
        if s <= 1:
            return None

        if t_base <= 0:
            return 0.0

        pure_computation_time = t_actual - c_startup
        normalized_time = pure_computation_time / (gamma * t_base)

        p_calc = (s / (s - 1)) * (1 - normalized_time)

        return max(0.01, min(0.99, p_calc))  # clamp to [0.01, 0.99]

    @staticmethod
    def update_p_moving_average(old_p: float, current_p: Optional[float], alpha: float):
        """
        Update p, based on Online Empirical Average update rule
        :return:
        """
        if current_p is None:
            return old_p

        return alpha * old_p + (1 - alpha) * current_p

    