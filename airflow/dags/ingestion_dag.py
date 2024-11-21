from datetime import timedelta
import datetime

import boto3

from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator

from airflow.utils.dates import days_ago
from airflow import DAG

import pandas as pd
import numpy as np

import requests

import yfinance as yf

from scripts.connections import get_secret
from scripts.connections import connect_to_snowflake
from snowflake.connector.pandas_tools import write_pandas


args = {
    'owner': "Garett",
    'depends_on_past': False,
    "start_date": days_ago(1),
    "email": "gkaube@outlook.com",
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}


def fix_yahoo_finance(df):
    """Yahoo Finance has a bug with converting time zones once 00:00 UTC is hit
    causing Yahoo Finance to have a missing date before the latest UTC date.
    I.E, the current date of your time zone may be missing.
    """
    df = df.copy()
    end_date = pd.to_datetime(df.index[-1])
    try:
        df.loc[datetime.date.today()]   
    except Exception as e:
        df.index.name = 'date'
        df = df.reset_index(drop=False)
        df.loc[df.index[-1], 'date'] = end_date - datetime.timedelta(days=1)
        df = df.set_index('date', drop=True)
    return df


def get_ticker(ticker:str, data_start:str):
    tick_obj = yf.Ticker(ticker)
    
    df = yf.download(ticker, start=data_start, interval="1d")
    try:
        df.columns = df.columns.droplevel(1)
    except Exception:
        pass

    df.columns = df.columns.str.lower()
    df.index.name = df.index.name.lower()

    # If data_start is the previous day,
    # the data point on data_start day will already be in the database
    # by definition so we want to drop it (first row of df)
    if (pd.to_datetime(data_start) != datetime.date.today()):
        df = df.iloc[1:]

    # Yahoo Finance has a bug with converting time zones once 00:00 UTC is hit
    # causing Yahoo Finance to have a missing date before the latest UTC date
    df = fix_yahoo_finance(df)
    return df, tick_obj


def add_basic_info(df:pd.DataFrame, ticker:yf.Ticker, symb:str):
    df = df.copy()
    info = ticker.get_info()
    df['symbol'] = symb
    df['currency'] = info['currency']
    df['exchange'] = info['exchange']
    return df


def get_dates_from_snowflake(ti, dest_table:str):
     username, password, account_id = ti.xcom_pull(task_ids="Get_Snowflake_Secrets")
     cur, conn = connect_to_snowflake(
         username, password, account_id, schema='STAGING'
     )
    
     # Get latest date that the data contains
     cur.execute(f"SELECT MAX(DATE) FROM {dest_table}")
     latest_date = cur.fetch_pandas_all()
     data_start = latest_date["MAX(DATE)"].item()

     cur.close()
     conn.close()   

     return data_start


def extract_data_from_source(ti, tickers:list):
    """Uses yfinance to retrieve OHLCV data for each ticker in the tickers list
    and combines the dataframes into one along axis 0
    
    """
    data_start = ti.xcom_pull(task_ids="Get_Recent_Date")

    # Retreive tickers OHLC data and combine them
    ticker_dfs = []
    for code in tickers:
        code_cur = tickers[code]
        raw, ticker = get_ticker(code_cur, data_start)
        raw = add_basic_info(raw, ticker, code)
        raw.index.name = 'date'
        ticker_dfs.append(raw)

    data = pd.concat(ticker_dfs, axis=0).reset_index()
    data["actual"] = np.nan
    data['prediction'] = np.nan

    try:
        data['date'] = data['date'].dt.tz_localize(None)
    except Exception as e:
        print(e)

    return data


def check_upload(ti, dest_table):
    username, password, account_id = ti.xcom_pull(task_ids="Get_Snowflake_Secrets")
    cur, conn = connect_to_snowflake(username, password, account_id,
                                       schema='STAGING')
    data_start = ti.xcom_pull(task_ids="Get_Recent_Date")

    data = ti.xcom_pull(task_ids="Extract_From_Source")
    
    upload = True

    if data_start != datetime.date.today():
        # Get the dates present in Snowflake
        cur.execute(f"SELECT DATE FROM {dest_table}")
        all_dates = cur.fetch_pandas_all()

        # Check if there is overlap with the new data and data that is already stored
        upload = not data['date'].isin(all_dates).any()

        if upload:    
            if (data[data.columns.drop(["actual", "prediction"])] == np.nan).any().any():
                print("data contains nans")
            else:
                dates = data["date"] 
                success = False
                # Load data to snowflake if conditions are met
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
    cur.close()
    conn.close()


def load_to_snowflake(data, conn, dest_table:str) -> None:
    if "date" in data.columns:
        data['date'] = data['date'].astype('str')
    data.columns = data.columns.str.upper()
    data.columns = data.columns.str.replace(" ", "_")
    success, nchunks, nrows, _ = write_pandas(conn, data, dest_table)
    return success


def update_current_data(ti, dest_table):
    """Updates the close prices etc of todays data"""
    username, password, account_id = ti.xcom_pull(task_ids="Get_Snowflake_Secrets")
    cur, conn = connect_to_snowflake(username, password, account_id,
                                       schema='STAGING')
    latest_date = ti.xcom_pull(task_ids="Get_Recent_Date")
    data = ti.xcom_pull(task_ids="Extract_From_Source")

    date = latest_date

    try:
        data['date'] = data['date'].dt.tz_localize(None)
    except Exception as e:
        print(e)

    if (latest_date == datetime.datetime.today().date()):
        print("deleting data")
        cur.execute(f"""
                    DELETE FROM {dest_table} WHERE DATE = TO_DATE('{date}', 'YYYY-MM-DD')
                    """)
        print("updating data")
        print("loading data:\n", data[data['date']==pd.to_datetime(latest_date)])
        success = load_to_snowflake(
            data[data['date']==pd.to_datetime(latest_date)], 
            conn, 
            dest_table
        )
        if success:
            print(f"Sucessfuly updated data with date: {latest_date}.")
    cur.close()
    conn.close()


def trigger_dbt_job(ti):
    """
    This currently does not work because 
    you must pay for api access
    """
    import time
    token, account_id = ti.xcom_pull(task_ids="Get_DBT_Token")
    job_id = "70403104225100"

    headers = {
        'Authorization': f'Token {token}',
        'Content-Type': 'application/json',
    }

    url = f'https://gc112.us1.dbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/'

    pass


with DAG(
    dag_id="ingestion_to_snowflake",
    description="",
    default_args=args,
    catchup=False,
    start_date=datetime.datetime(2024, 5, 16),
    schedule='15 00 * * *',
    is_paused_upon_creation=False
) as ingestion:
    
    dest_table= "ASSET_SOURCE"
    dbt_path = "/opt/airflow/dbt"
    dbt_profile = "my_dbt_project"
    username, password, account_id = get_secret("snowflake_connection")

    tickers = {
        "BTC" : "BTC-USD",
        "ETH": "ETH-USD",
        # CODE: CODE-CURRENCY
    }

    sec = PythonOperator(
        task_id="Get_Snowflake_Secrets",
        python_callable=get_secret,
        op_kwargs={"secret_name": "snowflake_connection"}
    )

    dates = PythonOperator(
        task_id="Get_Recent_Date",
        python_callable=get_dates_from_snowflake,
        op_kwargs={"dest_table": dest_table}
    )

    extract = PythonOperator(
        task_id="Extract_From_Source",
        python_callable=extract_data_from_source,
        op_kwargs={"tickers": tickers}
    ),

    upload = PythonOperator(
        task_id="Check_if_Upload",
        python_callable=check_upload,
        op_kwargs={"dest_table": dest_table}
    )

    update = PythonOperator(
        task_id="Update_Recent_Data",
        python_callable=update_current_data,
        op_kwargs={"dest_table": dest_table}
    )

    command = lambda x: f"/usr/local/airflow/python3-virtualenv/dbt-env/bin/dbt {x} --project-dir /tmp/dbt/my_dbt_project --profiles-dir /tmp/dbt/my_dbt_project --vars '{{\"DBT_USER\": \"{username}\", \"DBT_PASSWORD\": \"{password}\", \"DBT_HOST\": \"{account_id}\"}}'"
    
    # dbt_build = BashOperator(
    #     task_id="dbt_build",
    #     bash_command=f"source /usr/local/airflow/python3-virtualenv/dbt-env/bin/activate;\
    #         cp -R /usr/local/airflow/dags/dbt /tmp;\
    #         echo 'listing project files:';\
    #         ls -R /tmp;\
    #         cd /tmp/dbt/my_dbt_project;\
    #         {command('deps')};\
    #         {command('seed')};\
    #         {command('run')};\
    #         {command('test')};\
    #         cat /tmp/dbt_logs/dbt.log;\
    #         rm -rf /tmp/dbt/my_dbt_project"
    #     )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command = "dbt deps --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command = f"dbt seed --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --vars '{{\"DBT_USER\": \"{username}\", \"DBT_PASSWORD\": \"{password}\", \"DBT_HOST\": \"{account_id}\"}}'"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command = f"dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --vars '{{\"DBT_USER\": \"{username}\", \"DBT_PASSWORD\": \"{password}\", \"DBT_HOST\": \"{account_id}\"}}'"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command = f"dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --vars '{{\"DBT_USER\": \"{username}\", \"DBT_PASSWORD\": \"{password}\", \"DBT_HOST\": \"{account_id}\"}}'"
    )
    

    feature_engineering = TriggerDagRunOperator(
        task_id="Trigger_feature_engineering",
        trigger_dag_id="feature_engineering"
    )

sec >> dates >> extract  >> upload >> update >> dbt_deps >> dbt_seed >> dbt_run >> dbt_test >> feature_engineering