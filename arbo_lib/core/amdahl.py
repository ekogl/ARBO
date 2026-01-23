# TODO: implement logic from method.pdf

class AmdahlUtils:

    def calculate_theoretical_time(self):
        """
        Calculate theoretical time based on Amdahl's law (T_Amdahl)
        :return:
        """
        raise NotImplemented

    def calculate_observed_p(self):
        """
        Calculate observed p from initial 2 executions
        :return:
        """
        raise NotImplemented

    def update_p_moving_average(self):
        """
        Update p, based on Online Empirical Average update rule
        :return:
        """
        raise NotImplemented

    