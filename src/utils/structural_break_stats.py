import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller
from multiprocessing import Pool
from functools import partial
import logging
from tqdm import tqdm
import argparse
import os

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger=logging.getLogger(__name__)

def get_statistic(start, series, min_window, window_len):
    adfs = []
    end = start + window_len
    subset = series.iloc[start:end]
    logger.info(f"Computing SADF for {start}:{end}")
    for t0 in range(0, len(subset) - min_window + 1):
        # Move start forward
        subset_2 = subset.iloc[t0:]
        try:
            adf_stat = adfuller(subset_2, regression='c')[0]
        except ValueError as e:
            print(e)
            adf_stat = np.nan
        
        adfs.append(adf_stat)
    return np.max(adfs)


def sadf_test(series, min_window=20, window_len=50):
    """
    Calculates Supremum ADF (SADF) test statistics for each time step.
    
    Parameters
    ----------
    - series: pandas Series, the time series data to test.
    - min_window: int, the minimum window size for the rolling ADF tests.
    
    Returns
    -------
    - sadf_stat: float, the SADF test statistic.
    """
    sadfs_t = [np.NaN]*(window_len - 1)

    # Perform ADF test over a rolling window
    with Pool(12) as pool:
        stat = partial(get_statistic, series=series, min_window=min_window, window_len=window_len)
        result = pool.map(stat, range(len(series) - window_len+1))
        sadfs_t += result
    # Return the SADF statistics
    return sadfs_t


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--data_path", dest="data_path", default="./notebooks/nvda_daily.csv")

    args = arg_parser.parse_args()

    data = pd.read_csv(args.data_path, parse_dates=['date'], index_col=0)
    close = data.close
    logger.info(f"Computing statistics for {close.shape[0]} data points")
    statistics = sadf_test(np.log(close), min_window=20, window_len=100)
    print(statistics[-10:])
    print(len(statistics))
    stat_series = pd.Series(statistics, index=close.index)
    logger.info(f"saving to struct_break_stats3.csv")
    stat_series.to_csv("struct_break_stats_daily.csv")
    logger.info(f"Done")
