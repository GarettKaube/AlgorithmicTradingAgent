"""
Small script to generate initial tables for Snowflake
"""

import pandas as pd
import numpy as np
import yfinance as yf



init_tables_save_path = "./snowflake/init_csv_tables/"
data_start = "2017-11-09"

# Retrieve Bitcoin data
btc = yf.Ticker("BTC-USD")
raw = yf.download("BTC-USD", start=data_start, end = "2023-12-01",interval="1d")
raw.columns = raw.columns.str.lower()
raw.index.name = raw.index.name.lower()


# Retrieve Etherium data
eth = yf.Ticker("ETH-USD")
raw_eth = yf.download("ETH-USD", start=data_start, end = "2023-12-01", interval="1d")
raw_eth.columns = raw_eth.columns.str.lower()
raw_eth.index.name = raw_eth.index.name.lower()


def add_basic_info(df, ticker:yf.Ticker, symb):
    df = df.copy()
    info = ticker.get_info()
    df['symbol'] = symb
    df['currency'] = info['currency']
    df['exchange'] = info['exchange']
    df['timezone'] = info['timeZoneFullName']
    df['name'] = info['name']
    df['sector'] = info.get('sectorKey', np.nan)
    df['industry'] = info.get('industryKey', np.nan)
    return df


raw = add_basic_info(raw, btc, symb='BTC')
raw_eth = add_basic_info(raw_eth, eth, symb='ETH')

data = pd.concat([raw, raw_eth],axis=0).reset_index().reset_index()
data = data.rename({"index": "id"},axis=1)

data['prediction'] = np.nan
data['actual'] = np.nan
data['confidence'] = np.nan
data['model_id'] = np.nan


data['position_size'] = np.nan


data['account_id'] = np.nan
data['account_provider'] = np.nan
data['asset_type'] = "CRYPTOCURRENCY"


def generate_date_df(start ="2017-11-09", end="2030-01-01"):
    datetime_table = pd.DataFrame(pd.date_range(start, end, freq="D"), columns=['date'])
    datetime_table['year'] = datetime_table['date'].dt.year
    datetime_table['month'] = datetime_table['date'].dt.month
    datetime_table['day'] = datetime_table['date'].dt.day

    mapping = {date: i for i, date in enumerate(datetime_table['date'])}
    mapping_df  = datetime_table[['date']]

    datetime_table.drop('date', axis=1, inplace=True)
    datetime_table = datetime_table.reset_index()
    datetime_table = datetime_table.rename({"index":"date_id"}, axis=1)

    return datetime_table, mapping_df, mapping


datetime_table, mapping_df, mapping = generate_date_df()

mapping_df = mapping_df.reset_index()\
.rename({"index":"date_id", 0: "date"},axis=1)

mapping_df.to_csv(init_tables_save_path + "date_to_id_mapping.csv", index=False)
datetime_table.to_csv(init_tables_save_path + "datetime_dim.csv", index=False)

# --------- Fact tables -----------
ohlc_fact_table_data = data[["date", 'open', 'high', 'low', 
                        'close', 'adj close', 'volume', 
                        'symbol', 'currency', 'exchange', 
                        'prediction', 'actual', 'model_id']]


position_fact_table_data = pd.DataFrame(
    [["2020-01-07", "BTC", "USD", 5, 0.52312312,"a6gss56y2"]], 
    columns=["date", 'symbol', 'currency', 'position_size', 
             "trade_open_spread", 'account_id']
    )

account_balance_fact_table = pd.DataFrame(
    [["2020-01-07", "a6gss56y2", 10000]], 
    columns=["date", 'account_id', 'account_balance']
    )

prediction_fact_table = data[["date", 'prediction', 'confidence', 'actual', 'model_id']]


# Replace dates with their respective codes

ohlc_fact_table_data.to_csv(init_tables_save_path + "asset_fact_table.csv", index=False)
position_fact_table_data.to_csv(
    init_tables_save_path + "position_fact_table.csv", index=False
)
prediction_fact_table.to_csv(
    init_tables_save_path + "prediction_fact_table.csv", index=False
)
account_balance_fact_table.to_csv(
    init_tables_save_path + "account_balance_fact_table.csv", index=False
)

# Info for each asset
symbol_dimensional_data = data[["symbol", "exchange", "name", 
                                "sector", "industry", "asset_type"]]
symbol_dimensional_data = symbol_dimensional_data.drop_duplicates().reset_index(drop=True)
symbol_dimensional_data.to_csv(init_tables_save_path + "symbol_dimension.csv", index=False) 

# Exchanges
exchange_dim = pd.DataFrame([["CCC", "CoinMarketCap"]], 
                            columns=["exchange_code", "exchange_name"])
exchange_dim.to_csv(init_tables_save_path + "exchange_dim.csv", index=False)