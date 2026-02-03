import numpy as np
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import WhiteKernel, Matern, ConstantKernel as C
from typing import List, Dict

class ResidualModel:
    """
    Gaussian Process to learn the residual (error) of Amdahl's Law cannot explain
    Input (X): [Parallelism (s), Input Ścale (gamma), Cluster Load]
    Output (y): Residual time
    """

    def __init__(self):
        # ConstantKernel: learns size of the residual
        # Matern: learns shape, length_scale handles different units of s, gamma and load
        # WhiteKernel: learns noise level from last runs
        # kernel = C(1.0, (1e-3, 1e4)) * RBF([10, 1, 10], (1e-2, 1e2))
        kernel = C(1.0, (1e-3, 1e4)) * \
            Matern(length_scale=[10, 1, 10], nu=2.5, length_scale_bounds=(1e-2, 1e3)) + \
            WhiteKernel(noise_level=1.0, noise_level_bounds=(1e-2, 1e2))

        self.model = GaussianProcessRegressor(kernel=kernel, n_restarts_optimizer=5, alpha=1e-10, normalize_y=True)
        self.is_trained = False

    def train(self, history_rows: List[Dict]) -> None:
        """
        Trains GP on the historical data
        :param history_rows: list of dictionaries with execution history data
        :return: None
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
        :param s_candidates: list of candidate s values
        :param gamma: input scaling factor
        :param cluster_load: metric representing cluster load
        :return: predicted residuals
        """
        if not self.is_trained:
            return np.zeros(len(s_candidates))  # if no history, assume zero residuals

        X_pred = np.column_stack([
            s_candidates,
            np.full(len(s_candidates), gamma),
            np.full(len(s_candidates), cluster_load)
        ])

        return self.model.predict(X_pred)

