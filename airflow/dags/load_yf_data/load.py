""" Script to fill in date gaps in the OHLC fact table
"""
import pandas as pd
import numpy as np
import yfinance as yf 

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import datetime

import pytz
from datetime import time

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


def add_basic_info(df, ticker:yf.Ticker, symb):
    df = df.copy()
    info = ticker.get_info()
    df['symbol'] = symb
    df['currency'] = info['currency']
    df['exchange'] = info['exchange']
    return df


def connect_to_snowflake():
    conn = snowflake.connector.connect(
        user='bobjames',
        password='25382538Halo!',
        account='itzmlzs-cg90959',
        warehouse='COMPUTE_WH',
        database='DATA_ENG_DBT',
        schema='STAGING',
        role="DBT_EXECUTOR_ROLE"
    )

    cur = conn.cursor()

    return cur, conn


def get_ticker(ticker:str, data_start:str):
    tick_obj = yf.Ticker(ticker)
    df = yf.download(ticker, start=data_start, interval="1d")

    # Fix bug that the last data point skips a day
    df=df.reset_index()
    dates = df['Date']
    is_one_day_increment = (dates.iloc[-1] - dates.iloc[-2]) == pd.Timedelta(days=1)
    if not is_one_day_increment:
        dates.iloc[-1] = dates.iloc[-1] - pd.DateOffset(days=1)
    df = df.set_index('Date')
    
    df.columns = df.columns.str.lower()
    df.index.name = df.index.name.lower()
    return df, tick_obj


def load_to_snowflake(data, connection:snowflake.connector, dest_table:str) -> None:
    if "date" in data.columns:
        data['date'] = data['date'].astype('str')
    data.columns = data.columns.str.upper()
    data.columns = data.columns.str.replace(" ", "_")
    success, nchunks, nrows, _ = write_pandas(connection, data, dest_table)
    return success
    

def update_current_data(data, latest_date, cur, conn, dest_table):
    """Updates the close prices etc of todays data"""
    latest_date = latest_date["MAX(DATE)"]
    date = latest_date.item()
    if (latest_date.item() == datetime.datetime.today().date() 
        ):
        cur.execute(f"""DELETE FROM {dest_table} WHERE DATE = TO_DATE('{date}', 'YYYY-MM-DD')""")
        success = load_to_snowflake(data[data['date'].astype(str)==str(latest_date.item())], conn, dest_table)
        if success:
            print(f"Sucessfuly updated data with date: {latest_date}.")


def main():
    upload = True

    tickers = {
        "BTC" : "BTC-USD",
        "ETH": "ETH-USD",
        # CODE: CODE-CURRENCY
    }

    # Table to upload data to
    dest_table= "ASSET_SOURCE"

    cur, conn = connect_to_snowflake()
    
    cur.execute("CREATE SCHEMA IF NOT EXISTS DATA_ENG_DBT.STAGING;")

    # Get latest date that the data contains
    cur.execute(f"SELECT MAX(DATE) FROM {dest_table}")
    latest_date = cur.fetch_pandas_all()
    data_start = latest_date["MAX(DATE)"].item()

    logger.info(f"datastart at: {data_start}")

    ticker_dfs = []
    for code in tickers:
        code_cur = tickers[code]
        raw, ticker = get_ticker(code_cur, data_start)
        raw = add_basic_info(raw, ticker, code)

        # Skip last data point to remove incomplete data
        raw = raw.iloc[:-1]

        # Make sure the first datapoint is not the data_start
        # datapoint unless data_start is today
        if data_start != datetime.datetime.today().date():
            raw = raw.iloc[1:]
        ticker_dfs.append(raw)
    

    data = pd.concat(ticker_dfs, axis=0).reset_index()
    data["actual"] = np.nan
    data['prediction'] = np.nan

    # Note: We do not want to add any data that is realized on the same date
    # to avoid loading incomplete trading day data
    # The data point on the same day will be dealt with an
    # airflow job that will start after market close

    if data_start != datetime.datetime.today().date():
        # Load to snowflake

        cur.execute(f"SELECT DATE FROM {dest_table}")
        all_dates = cur.fetch_pandas_all()

        # Check if there is overlap with the new data and data that is already stored
        upload = not data['date'].isin(all_dates).any()

        if upload:    
            if (data[data.columns.drop(["actual", "prediction"])] == np.nan).any().any():
                logger.error("data contains nans")

            else:
                # Select the first dataframe (arbritrarily) and get the dates
                dates = data["date"] 
                success = False
                if dates.shape[0] == 1 and  dates.item() != pd.to_datetime(data_start):
                    success = load_to_snowflake(data, conn, dest_table)
                elif dates.shape[0] > 1:
                    success = load_to_snowflake(data, conn, dest_table)
                else:
                    print("Data already in table")
                if success:
                    print("Data loaded successfuly")
        
    else:
        print("Nothing to upload. Closing connection.")
    
    # Update todays data because todays data is incomplete until 00:00:00 UTC
    # (Daily crpyto bars close at 00:00:00 UTC)
    update_current_data(data, latest_date, cur, conn, dest_table)
        

    # Close Snowflake connection
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
