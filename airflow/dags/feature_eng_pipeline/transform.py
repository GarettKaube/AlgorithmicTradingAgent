import pandas as pd
import numpy as np
from ta.volatility import AverageTrueRange
from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator

from statsmodels.regression.rolling import RollingOLS
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint


from pytrends.request import TrendReq
import pandas as pd

def transform_data(ti):
     data = ti.xcom_pull(task_ids="Extract_Data_From_Snowflake", key=None)
     assets, _, _ = ti.xcom_pull(task_ids="Get_Variables", key=None)

     if data is None or assets is None:
        raise ValueError("Required data from XCom is missing")
     
     # Get the OHLC data for each symbol and concat the dfs
     asset_dfs = []
     for asset in assets:
          ad = data[data['SYMBOL_CODE'] == asset]
          # Date_code columns may be duplicated
          ad = ad.loc[:, ~ad.columns.duplicated()]
          ad.columns = [col + f"_{asset}" for col in ad.columns]
          asset_dfs.append(ad.reset_index(drop=True))

     data = pd.concat(asset_dfs, axis=1)

     year_col = [col for col in data.columns if "YEAR" in col][0] # [0] here is arbritrary
     month_col = [col for col in data.columns if "MONTH" in col][0]
     day_col = [col for col in data.columns if "DAY" in col][0]

     # Create full date column    
     data['DATE'] = data[[year_col, month_col, day_col]]\
        .apply(lambda x: pd.Timestamp(x[year_col], x[month_col], x[day_col]), axis=1)
     
     # Drop unneeded columns
     data = data.drop(
         [col for col in data.columns if "symbol_code" in col.lower()] + 
         [col for col in data.columns if "currency_code" in col.lower()] +
         [col for col in data.columns if "range" in col.lower()] +
         [col for col in data.columns if "exchange_code" in col.lower()],
         axis=1
     )
     data = data.set_index('DATE')
     return data


def entropy(data, base=2):
    """entropy estimator
    """
    _, count = np.unique(data, return_counts=True, axis=0)
    prob = count.astype(float) / len(data)
    prob = prob[prob > 0.0]
    return np.sum(prob * np.log(1.0 / prob)) / np.log(base)


def calculate_rolling_entropy(data:pd.Series, window=30):
    """
    Returns
    -------
    - pd.Series
    """
    entropy_ = data.rolling(window=window)\
        .apply(lambda x:entropy(x))
    return entropy_


def calculate_standardized_spread(ti, rolling_win_size=55):
        """
        Parameters
        ----------
        data: pd.DataFrame
            Contains the close1 and close2 variables to calculate the spread of
        
        Returns
        -------
        pd.Dataframe
        """
        data = ti.xcom_pull(task_ids="Transform_Snowflake_Data", key=None)
        _, closes, _ = ti.xcom_pull(task_ids="Get_Variables", key=None)

        close1, close2 = closes[1], closes[0]
        spread = data[close1].pct_change() - data[close2].pct_change()
        spread = spread.to_frame(name='spread')

        spread['mu'] = spread['spread'].rolling(window=rolling_win_size).mean()
        spread['std'] = spread['spread'].rolling(window=rolling_win_size).std()
        spread["standardized_spread"] = (spread['spread']- spread.mu) / spread['std']

        data = data.join(spread)
        return data


# Note that bt(close change is calculated in dbt)
def get_bt(data, close_var="close"):
    def compute_close_change(data, close_var):
        data_ = data.copy()
        change = data_[close_var].diff()\
            .replace({0:np.nan})\
            .ffill()\
            .dropna()
        return change
    
    data_ = data.copy()
    data_[f'close_change_{close_var}'] = compute_close_change(data, close_var)

    data_.loc[:,f"bt_{close_var}"] = np.sign(data_[f'close_change_{close_var}'])
    return data_


def calculate_entropy(ti):
     data = ti.xcom_pull(task_ids="Engineer_Spread_Features", key=None)
     _, _, bt_vars = ti.xcom_pull(task_ids="Get_Variables", key=None)
     data = data.copy()
     print(data.columns)
     for col in bt_vars:
          data[f"{col}_entropy"] = calculate_rolling_entropy(data[col])
     return data



def compute_bid_ask_spread(data, close_change_var):
    data_ = data.copy()

    if close_change_var not in data_.columns:
        print(f"Missing {close_change_var} column")

    data_[f'bid_ask_spread_{close_change_var}'] = data_[close_change_var]\
        .rolling(window=20)\
        .apply(
            lambda x: np.cov(x, x.shift(1).fillna(0))[0, 1]
        )

    data_[f'bid_ask_spread_{close_change_var}'] = np.sqrt(
        data_[f'bid_ask_spread_{close_change_var}'].apply(lambda x: max(0.0, -x))
    )

    return data_


def get_bid_ask_spread(ti):
     data = ti.xcom_pull(task_ids="Calculate_Entropy_Features")
     close_chg_vars = ti.xcom_pull(task_ids="Get_Close_Change_Variables")
     for col in close_chg_vars:
          data = compute_bid_ask_spread(data, col)
     return data



def compute_cointegration_stats(closes, close_vars, window=100):
    from statsmodels.tsa.stattools import coint
    def compute_coint(x):
        stat = coint(x[:,0], x[:,1], trend='n')[0]
        return stat
    
    closes = closes.copy()
    coint = closes.rolling(window=window)\
        .apply(
            lambda x : compute_coint(
                np.column_stack([closes[close_vars[1]].loc[x.index], 
                                 closes[close_vars[0]].loc[x.index]])
                ),
            raw=False)
    return coint


def get_coint_stats(ti):
     data = ti.xcom_pull(task_ids="Calculate_Bid_Ask_Spread_Features")
     _, vars, _= ti.xcom_pull(task_ids="Get_Variables")
     data[[var + "_coint" for var in vars]] = compute_cointegration_stats(
         data[vars], 
         vars, 
         window=100
     )
     return data


def standardize(x):
    return (x - x.mean()) / x.std()


def compute_vwap(close, high, low, volume):
    hlc3 = (close + high + low) / 3
    vol = hlc3 * volume 
    vwap = vol.groupby(vol.index.day).cumsum()\
        .div(hlc3.groupby(hlc3.index.day).cumsum())
    return vwap



def upper_crossover(data1, data2):
    shifted1 = data1.shift(1)
    shifted2 = data2.shift(1)

    upper = pd.concat([data1 >= shifted2, shifted1 < shifted2], 
                axis=1)\
            .all(axis=1)
    return upper.astype(int)
    
    
def lower_crossover(data1, data2):
    shifted1 = data1.shift(1)
    shifted2 = data2.shift(1)

    lower = pd.concat([data1 <= shifted2, shifted1 > shifted2], 
                axis=1)\
            .all(axis=1)
    return lower.astype(int)



def daily_volatility(data, h=100):
    returns = data.pct_change(freq=pd.Timedelta(days=1))
    rolling_std = returns.ewm(span=h).std()
    return rolling_std


def compute_rolling_ols_betas(data:pd.DataFrame, window=24):
    """Computes a rolling OLS of data on a trend and returns the betas."""
    betas = RollingOLS(
        endog=data,
        exog=sm.add_constant(
            pd.DataFrame(np.arange(data.shape[0]), index=data.index)
        ),
        window=window)\
        .fit(params_only=True)\
        .params\
        .dropna()\
        .drop(['const'], axis=1)
    return betas


def compute_technical_features(ti):
    data_ = ti.xcom_pull(task_ids="Engineer_Cointegration_Stat_Features")

    atr_btc = AverageTrueRange(data_['HIGH_BTC'], data_['LOW_BTC'], data_['CLOSE_BTC'])
    atr_eth = AverageTrueRange(data_['CLOSE_ETH'], data_['HIGH_ETH'], data_['CLOSE_ETH'])
    rsi = RSIIndicator(data_['standardized_spread'])

    for ma_len in [5, 20]:
        ema = EMAIndicator(data_['standardized_spread'], window=ma_len)
        data_[f"MA_{ma_len}"] = ema.ema_indicator()
    print(data_.info())
    data_["vwap_btc"] = compute_vwap(data_['CLOSE_BTC'], data_['HIGH_BTC'], 
                                     data_['LOW_BTC'], data_['VOLUME_BTC'])
    data_["vwap_eth"] = compute_vwap(data_['CLOSE_ETH'], data_['HIGH_ETH'], 
                                     data_['LOW_ETH'], data_['VOLUME_ETH'])
    data_["MA_upper_co"] = upper_crossover(data_["MA_5"], data_["MA_20"])
    data_["MA_lower_co"] = lower_crossover(data_["MA_5"], data_["MA_20"])
    data_['rsi'] = rsi.rsi()
    data_['atr_btc'] = atr_btc.average_true_range()
    data_['atr_eth'] = atr_eth.average_true_range()
    return data_


# Initialize pytrends
pytrends = TrendReq(hl='en-US', tz=360)

# Function to fetch data in chunks (due to daily limit for large periods)
def fetch_trends(keyword, start_date, end_date):
    timeframe = f'{start_date} {end_date}'
    pytrends.build_payload([keyword], cat=0, timeframe=timeframe, geo='', gprop='')
    data = pytrends.interest_over_time()
    return data


def get_trends_data(topic, start_date = '2016-09-01', end_date = '2025-09-01'):
    import time
    import random
    # Break the time period into smaller chunks
    chunks = pd.date_range(start=start_date, end=end_date, freq='9M')

    trends_data = pd.DataFrame()

    for i in range(len(chunks)-1):
        chunk_start = chunks[i].strftime('%Y-%m-%d')
        chunk_end = chunks[i+1].strftime('%Y-%m-%d')
        try:
            print(chunk_start, chunk_end)
            daily_data = fetch_trends(topic, chunk_start, chunk_end)
            trends_data = pd.concat([trends_data, daily_data])
            wait = random.randint(5, 10)
            print(f"waiting {wait} seconds")
            time.sleep(wait)
            print("success")
        except Exception as e:
            print(e)
            print("Limit reached. Waiting 5 minutes")
            time.sleep(60*5)
            try:
                daily_data = fetch_trends(topic, chunk_start, chunk_end)
                trends_data = pd.concat([trends_data, daily_data])
            except Exception as e:
                print(e)
    return trends_data


def lower_case(x):
    return x.lower()


def get_google_trends_data(ti):
    data = ti.xcom_pull(task_ids="Engineer_Technical_Features", key=None)
    sec_names = ti.xcom_pull(task_ids="Get_Symbol_Info", key=None)
    trends_end = data.index[-1]
    trends_start = trends_end - pd.DateOffset(months=10)
    
    trends_list = []
    for sec in sec_names:
        trend = get_trends_data(sec.lower(), 
                                start_date = trends_start, 
                                end_date = trends_end)
        try:
            trend = trend.drop('isPartial', axis=1)
        except KeyError:
            pass
        trend = trend[~trend.index.duplicated(keep='first')]
        trend.index.name = "DATE"
        trends_list.append(trend)
    
    data = data.join(trends_list)
    data[list(map(lower_case, sec_names))] = data[list(map(lower_case, sec_names))].ffill()

    return data