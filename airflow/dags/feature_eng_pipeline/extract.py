import sys
import os
#sys.path.insert(0, "./dags/scripts")

from scripts.connections import connect_to_snowflake

def extract_from_snowflake(ti, n_days=150):
     """ Extracts the latest n_days data from the ASSETS data mart
     """
     username, password, account_id = ti.xcom_pull(task_ids="Get_Secrets")
     cur, conn = connect_to_snowflake(username, password, account_id,
                                       schema='PROD_MART_ASSETS')
     
     # n_days==0 -> extract all data
     join = "DIM_DATE" if n_days == 0 else "LATEST_DATES"

     # Query to find the date_codes for the last 150 days
     # And join the OHLC data with those days to get the
     # latest 150 OHLC data
     # Note: the DATE_CODE column will be duplicated 
     cur.execute(f"""
                 WITH LATEST_DATES AS (
                    SELECT DISTINCT
                        ohlc.DATE_CODE,
                        YEAR,
                        MONTH,
                        DAY
                    FROM FACT_OHLC as ohlc
                    JOIN DIM_DATE as dates ON
                        ohlc.DATE_CODE = dates.DATE_CODE
                    WHERE ohlc.DATE_CODE >= (
                        SELECT MAX(DATE_CODE) FROM FACT_OHLC
                        ) - {n_days}
                )

                SELECT 
                    CAST(ohlc.OPEN AS FLOAT) as OPEN,
                    CAST(ohlc.HIGH AS FLOAT) as HIGH,
                    CAST(ohlc.CLOSE AS FLOAT) as CLOSE,
                    CAST(ohlc.ADJUSTED_CLOSE AS FLOAT) as ADJUSTED_CLOSE,
                    CAST(ohlc.LOW AS FLOAT) as LOW,
                    ohlc.VOLUME,
                    ohlc.SYMBOL_CODE,
                    ohlc.EXCHANGE_CODE,
                    ohlc.CURRENCY_CODE,
                    ohlc.DATE_CODE,
                    ohlc.DOLLAR_VOLUME,
                    ohlc.DOLLAR_VOLUME1M,
                    ohlc.RANGE,
                    ohlc.TRUE_RANGE,
                    ohlc.CLOSE_CHANGE,
                    ohlc.CLOSE_DIRECTION,
                    dates.YEAR,
                    dates.MONTH,
                    dates.DAY
                FROM FACT_OHLC as ohlc
                JOIN {join} as dates ON
                    ohlc.DATE_CODE = dates.DATE_CODE;
                 """
                )
     
     data = cur.fetch_pandas_all()
     
     data['HIGH'] = data['HIGH'].astype(float)
     data['LOW'] = data['LOW'].astype(float)
     data['CLOSE'] = data['CLOSE'].astype(float)

     cur.close()
     conn.close()
     return data


def get_symbol_info(ti):
    username, password, account_id = ti.xcom_pull(task_ids="Get_Secrets")
    cur, conn = connect_to_snowflake(username, password, account_id, 
                                     schema='PROD_MART_ASSETS')
    cur.execute(
        """
        SELECT
            SYMBOL_CODE,
            SECURITY_NAME
        FROM DIM_SYMBOL
        """
    )
    symbol_info = cur.fetch_pandas_all()

    cur.close()
    conn.close()
    return list(symbol_info["SECURITY_NAME"])


