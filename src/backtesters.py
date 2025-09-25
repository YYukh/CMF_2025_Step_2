import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
from scipy.stats import kurtosis, norm, skew


days_in_year = 365.25

def Return(rets):
    """
    Annual return estimate

    :rets: daily returns of the strategy
    """
    return np.mean(rets, axis=0)*days_in_year


def Volatility(rets):
    """
    Estimation of annual volatility

    :rets: daily returns of the strategy
    """
    return np.std(rets, axis=0)*np.sqrt(days_in_year)


def SharpeRatio(rets):
    """
    Estimating the annual Sharpe ratio

    :rets: daily returns of the strategy
    """
    volatility = Volatility(rets)
    return Return(rets)/volatility
    
def ROI(rets):
    """
    Total Return (ROI) over the entire period.
    Compounded growth: (1 + r1)(1 + r2)...(1 + rn) - 1

    :rets: daily returns of the strategy
    """
    return np.prod(1 + rets) - 1

def MaxDrawdown(rets):
    """
    Maximum drawdown over the period.
    
    :rets: daily returns of the strategy
    """
    wealth_index = (1 + rets).cumprod()
    rolling_max = wealth_index.cummax()
    drawdowns = (wealth_index - rolling_max) / rolling_max
    return drawdowns.min()  # наибольшая отрицательная просадка

def CalmarRatio(rets):
    """
    Calmar Ratio = Annual Return / |Max Drawdown|
    Uses absolute value of max drawdown (as positive denominator).

    :rets: daily returns of the strategy
    """
    mdd = MaxDrawdown(rets)
    if abs(mdd) > 1e-8:  # защита от деления на 0
        return Return(rets) / abs(mdd)
    else:
        return float('NaN')
    
def SortinoRatio(rets):
    """
    Sortino Ratio: Annualized return / downside deviation
    
    Downside deviation = std of negative returns only, annualized.
    
    :rets: daily returns of the strategy
    """
    target = 0.0  # можно задать минимально приемлемую доходность
    downside = rets[rets < target]
    
    if len(downside) == 0:
        downside_vol = 0.0
    else:
        downside_vol = np.std(downside) * np.sqrt(days_in_year)
    
    if downside_vol > 1e-8:
        return Return(rets) / downside_vol
    else:
        return float('NaN')

def statistics_calc(rets, bh, name = '_', plot = False):
    """
    Draws a graph of portfolio equity and calculates annual Sharpe ratios, profitability and volatility

    :rets: daily returns of the strategy
    """
    sharpe = SharpeRatio(rets)
    sortino = SortinoRatio(rets)
    calmar = CalmarRatio(rets)
    ret = Return(rets)
    roi = ROI(rets)
    vol = Volatility(rets)
    md = MaxDrawdown(rets)
    
    if plot:
        plt.plot(rets.cumsum(), label = 'strategy')
        plt.plot(bh.cumsum(), label = 'buy & hold')
        plt.xlabel('t')
        plt.legend()
        print('Sharpe ratio = %0.2f'%sharpe)
        print('Sortino ratio = %0.2f'%sortino)
        print('Calmar ratio = %0.2f'%calmar)
        print('Annual Return = %0.2f'%ret)
        print('ROI = %0.2f'%roi)
        print('Annual Std = %0.2f'%vol)
        print('Max Drawdown = %0.2f'%md)
    return  pd.DataFrame([[sharpe, sortino, calmar, ret, roi, vol, md]], columns = ['Sharpe ratio', 'Sortino ratio', 'Calmar ratio', 'Annual return', 'ROI', 'Volatility', 'Max Drawdown'], index = [name])


def prob_sharpe(returns, sr_tested, sr_etalon=0):
    """
    The probabilistic Sharpe ratio (PSR) provides an adjusted estimate of SR, by removing
    the inflationary effect caused by short series with skewed and/or fat-tailed returns

    Given a user-defined benchmark Sharpe ratio (sr_etalon) and an observed Sharpe ratio SR (sr_tested) estimates the probability that sr_tested is greater than sr_etalon

    It should exceed 0.95, for the standard significance level of 5%

    Args:
        strategy (pd.Series): equity curve of the strategy
        sr_tested (float): sharpe ratio to test
        sr_etalon (float): sharpe ratio to compare. Defaults to 0.

    Returns:
        float: probability that sr_tested is greater than sr_etalon
    """
    strategy = pd.DataFrame(returns)
    y3 = skew(returns)
    y4 = kurtosis(returns) + 3
    stat = (
        (sr_tested - sr_etalon)
        * math.sqrt(len(strategy) - 1)
        / math.sqrt(1 - y3 * sr_tested + (y4 - 1) * sr_tested / 4)
    )
    return norm.cdf(stat)


def deflated_sharpe(returns, bagging_strategies, sr_tested=0):
    """
    The deflated Sharpe ratio (DSR) is a PSR where the rejection threshold is adjusted to
    reflect the multiplicity of trials

    Corrects SR for inflationary effects caused by non-Normal returns, track record length, and multiple testing/selection bias.

    It should exceed 0.95, for the standard significance level of 5%

    Args:
        strategy (_type_): equity curve of the strategy
        bagging_strategies (float): _description_
        sr_tested (float): sharpe ratio to compare. Defaults to 0

    Returns:
        float: probabilistic Sharpe ratio adjusted for multiplicity of trials
    """
    y = (
        round((1.0 - math.gamma(1 + 1.0e-8)) * 1.0e14) * 1.0e-6
    )  # Euler-Mascheroni constant
    sharpes = SharpeRatio(bagging_strategies)
    N = bagging_strategies.shape[1]
    sr_std = sharpes.std()
    sr_etalon = sr_std * (
        (1 - y) / norm.cdf(1 - 1 / N) + y / norm.cdf(1 - 1 / (N * math.e))
    )
    return prob_sharpe(returns, sr_tested, sr_etalon)