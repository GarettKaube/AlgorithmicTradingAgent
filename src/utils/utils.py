import snowflake.connector
import yfinance as yf
import json

def connect_to_snowflake(schema):
    """ Connects to snowflake with schema
    Parameters
    ----------
    - schema: str
      name of schema to use
    
    Returns
    -------
    - cur
      snowflake cursor
    - conn
      snowflake connection
    """

    with open("./config/snowflake_credentials.json", 'r') as f:
      creds = json.load(f)
      sf_username = creds['sf_username']
      sf_password = creds['sf_password']
      sf_account_id = creds['sf_account_id']
      
      conn = snowflake.connector.connect(
        user=sf_username,
        password=sf_password,
        account=sf_account_id,
        warehouse='COMPUTE_WH',
        database='DATA_ENG_DBT',
        schema=schema,
        role="DBT_EXECUTOR_ROLE"
    )

    cur = conn.cursor()

    return cur, conn


def get_ticker(ticker:str, data_start:str, interval="1d"):
    tick_obj = yf.Ticker(ticker)
    df = yf.download(ticker, start=data_start, interval=interval)
    
    try:
        df.columns = df.columns.droplevel(1)
    except Exception:
        pass

    df.columns = df.columns.str.lower()
    df.index.name = df.index.name.lower()

    return df