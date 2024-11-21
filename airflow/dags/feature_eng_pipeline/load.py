from scripts.connections import connect_to_snowflake
from snowflake.connector.pandas_tools import write_pandas

expected_features = ['DATE', 'OPEN_ETH', 'HIGH_ETH', 'CLOSE_ETH', 'ADJUSTED_CLOSE_ETH', 
                     'LOW_ETH',
                'VOLUME_ETH', 'DATE_CODE_ETH', 'DOLLAR_VOLUME_ETH',
                'DOLLAR_VOLUME1M_ETH', 'CLOSE_CHANGE_ETH', 'CLOSE_DIRECTION_ETH',
                'YEAR_ETH', 'MONTH_ETH', 'DAY_ETH', 'OPEN_BTC', 'HIGH_BTC', 'CLOSE_BTC',
                'ADJUSTED_CLOSE_BTC', 'LOW_BTC', 'VOLUME_BTC', 'DATE_CODE_BTC',
                'DOLLAR_VOLUME_BTC', 'DOLLAR_VOLUME1M_BTC', 'CLOSE_CHANGE_BTC',
                'CLOSE_DIRECTION_BTC', 'spread',
                'mu', 'std', 'standardized_spread', 'CLOSE_DIRECTION_ETH_entropy',
                'CLOSE_DIRECTION_BTC_entropy', 'bid_ask_spread_CLOSE_CHANGE_ETH',
                'bid_ask_spread_CLOSE_CHANGE_BTC', 'CLOSE_ETH_coint', 'CLOSE_BTC_coint',
                'MA_5', 'MA_20', 'vwap_btc', 'vwap_eth', 'MA_upper_co', 'MA_lower_co',
                'rsi', 'atr_btc', 'atr_eth']


def create_features_schema_and_table(ti, dest_schema='FEATURES'):
    """ Creates the destination table for the features
    """
    # Create a string of the datas data types
    data_types = [f"{fet} NUMBER(38,14)" for fet in expected_features 
                  if fet.lower() != 'date']
    data_types = ["DATE VARCHAR(16777216)"] + data_types
    data_types_str = ",\n".join(data_types)
    
    username, password, account_id = ti.xcom_pull(task_ids="Get_Secrets")
    cur, conn = connect_to_snowflake(username, password, account_id,
                                     schema=dest_schema)
    # Create schema and table
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS  DATA_ENG_DBT.{dest_schema};")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS DATA_ENG_DBT.{dest_schema}.FEATURES (
                {data_types_str}
        );

    """)
    cur.close()
    conn.close()



def load_features_to_snowflake(ti, dest_schema='FEATURES'):
    username, password, account_id = ti.xcom_pull(task_ids="Get_Secrets")
    features = ti.xcom_pull(task_ids="Engineer_Technical_Features")
    cur, conn = connect_to_snowflake(username, password, account_id, dest_schema)

    print(features.columns)
    features = features.reset_index()
    print(features)
    features = features[expected_features]
    print(features)

    if "DATE" in features.columns:
        features['DATE'] = features['DATE'].astype('str')
    features.columns = features.columns.str.upper()
    features.columns = features.columns.str.replace(" ", "_")

    success, nchunks, nrows, _ = write_pandas(conn, features, "FEATURES")

    cur.close()
    conn.close()
    return success