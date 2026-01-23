# TODO: implement estimator logic, will use amdahl and residual

class ArboEstimator:
    def __init__(self):
        pass

    def predict_total_time(self):
        """
        Combines AmdahlUtils.calculate_time + ResidualModel.predict
        :return:
        """
        raise NotImplemented

    def optimize(self):
        """
        Optimization Loop (Phase 2 of proposed algorithm)
        :return:
        """
        raise NotImplemented