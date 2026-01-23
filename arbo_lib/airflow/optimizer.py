# TODO: handle integration with Airflow (using Option B (JIT))

class ArboOptimizer:
    def __init__(self):
        pass

    def get_optimal_parallelism(self):
        """
        Finds out optimal parallelism
        :return:
        """
        raise NotImplemented

    def feedback_control(self):
        """
        Callback, extract T_actual, s, gamma; calculate p_current and residual R; update DB
        :return:
        """
        raise NotImplemented