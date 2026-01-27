import numpy as np
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF, ConstantKernel as C
from typing import List, Dict

class ResidualModel:
    """
    Gaussiona Process to learn the residual (error) of Amdahl's Law cannot explain
    Input (X): [Parallelism (s), Input Ścale (gamma), Cluster Load]
    Output (y): Residual time
    """

    def __init__(self):
        kernel = C(1.0, (1e-3, 1e4)) * RBF([10, 1, 10], (1e-2, 1e2))
        self.model = GaussianProcessRegressor(kernel=kernel, n_restarts_optimizer=2, alpha=1.0)
        self.is_trained = False

    def train(self, history_rows: List[Dict]) -> None:
        """
        Trains GP on the historical data
        :return:
        """
        if not history_rows:
            self.is_trained = False
            return

        X = []
        y = []
        for row in history_rows:
            X.append([row["parallelism"], row["input_scale_factor"], row["cluster_load"]])
            y.append(row["residual"])

        self.model.fit(np.array(X), np.array(y))
        self.is_trained = True

    def predict(self, s_candidates: np.ndarray, gamma: float, cluster_load: float) -> np.ndarray:
        """
        Predicts residual for a list of candiate 's' values
        :return:
        """
        if not self.is_trained:
            return np.zeros(len(s_candidates))  # if no history, assume zero residuals

        X_pred = np.column_stack([
            s_candidates,
            np.full(len(s_candidates), gamma),
            np.full(len(s_candidates), cluster_load)
        ])

        return self.model.predict(X_pred)

