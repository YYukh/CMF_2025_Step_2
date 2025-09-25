import numpy as np
import pandas as pd
from sklearn.mixture import GaussianMixture


def make_gmm_simulations(close, n_samples=None, n_iter=10, n_components=20):
    """Generates synthetic price trajectories based on GMM estimation

    Args:
        close (pd.Series): close price
        n_samples (int): length of generated series
        n_iter (int): number of generated series. Defaults to 10.
        n_components (int): number of GMM components. Defaults to 20.

    Returns:
        pd.DataFrame: dataframe of synthetic price trajectories
    """
    if n_samples is None:
        n_samples = len(close)

    returns = close.pct_change().fillna(0)

    if isinstance(returns, pd.Series):
        returns = pd.DataFrame(returns.squeeze())

    return_simulations = np.array(
        [
            np.array(
                GaussianMixture(
                    n_components=n_components,
                    covariance_type="full",
                    n_init=1,
                    random_state=i,
                )
                .fit(returns)
                .sample(n_samples=n_samples)[0]
            )
            for i in range(n_iter)
        ]
    )

    return_simulations = np.squeeze(return_simulations).T

    return_simulations = pd.DataFrame(
        return_simulations, columns=range(return_simulations.shape[1])
    )

    return_simulations.iloc[0, :] = 0

    simulated_prices = pd.DataFrame(
        (return_simulations + 1).cumprod(),
        columns=range(return_simulations.shape[1]),
    )

    return simulated_prices
