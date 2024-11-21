import pytest
from unittest import mock
from airflow.models import DagBag

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

import json

import sys
sys.path.insert(0, "./airflow/dags")
from scripts.connections import connect_to_snowflake
from feature_eng_pipeline.transform import (transform_data, calculate_standardized_spread,
                  calculate_entropy, get_google_trends_data)                          
                                            


dagbag_path="airflow/dags"

with open("./config/snowflake_credentials.json", 'r') as f:
    creds = json.load(f)
    sf_username = creds['sf_username']
    sf_password = creds['sf_password']
    test_schema = creds['test_schema']
    sf_account_id = creds['sf_account_id']


@pytest.fixture(scope="module")
def dagbag():
    dagbag = DagBag(
        dag_folder=dagbag_path, 
        include_examples=False
    )
    return dagbag


@pytest.fixture
def snowflake_connection():
    cur, conn = connect_to_snowflake(sf_username, sf_password, sf_account_id, 
                         schema=test_schema)
    
    return cur


LOOKBACK_LEN =  150

@pytest.fixture
def test_assets():
    return ["BTC", "ETH"]

@pytest.fixture
def transformed_data():
    return pd.read_csv(
            "./tests/test_airflow_dags/test_feature_engineering_dag/fixtures/test_transform_data.csv", 
            parse_dates=["DATE"],
            index_col=0
        )


@pytest.fixture
def test_close_data():
    test_data = pd.DataFrame(
        {
            'CLOSE_ETH': [ 32, 5, 1, 6, 4, 7],
            'CLOSE_BTC': [5, 4, 8, 43, 9, 2] 
        }
    )
    return test_data



def test_extract(snowflake_connection):
    cur = snowflake_connection
    cur.execute(f"""
                 WITH LATEST_DATES AS (
                    SELECT DISTINCT
                        ohlc.DATE_CODE,
                        dates.YEAR,
                        dates.MONTH,
                        dates.DAY
                    FROM TEST_OHLC as ohlc
                    JOIN TEST_DIM_DATES as dates ON
                        ohlc.DATE_CODE = dates.DATE_CODE
                    WHERE ohlc.DATE_CODE >= (
                        SELECT MAX(DATE_CODE) FROM TEST_OHLC
                        ) - {LOOKBACK_LEN})
                SELECT * FROM LATEST_DATES;
                """
                )
    data = cur.fetch_pandas_all()
    assert type(data) == pd.DataFrame
    assert data.shape[0] == (LOOKBACK_LEN+1)
    assert "DATE_CODE" in data.columns
    assert (set(["YEAR", "MONTH", "DAY"]).intersection(set(data.columns)) 
            == set(["YEAR", "MONTH", "DAY"]))
    assert data["DATE_CODE"].sort_values().to_list() == list(range(160-LOOKBACK_LEN, 161))
    assert list(data["YEAR"].unique()) == [2018, 2017]


    cur.execute(f"""
                 WITH LATEST_DATES AS (
                    SELECT DISTINCT
                        ohlc.DATE_CODE,
                        dates.YEAR,
                        dates.MONTH,
                        dates.DAY
                    FROM TEST_OHLC as ohlc
                    JOIN TEST_DIM_DATES as dates ON
                        ohlc.DATE_CODE = dates.DATE_CODE
                    WHERE ohlc.DATE_CODE >= (
                        SELECT MAX(DATE_CODE) FROM TEST_OHLC
                        ) - {LOOKBACK_LEN}
                )

                SELECT 
                    ohlc.OPEN,
                    ohlc.HIGH,
                    ohlc.CLOSE,
                    ohlc.ADJUSTED_CLOSE,
                    ohlc.LOW,
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
                FROM TEST_OHLC as ohlc
                JOIN LATEST_DATES as dates ON
                    ohlc.DATE_CODE = dates.DATE_CODE;
                 """
                )
     
    data = cur.fetch_pandas_all()

    assert type(data) == pd.DataFrame
    assert 'SYMBOL_CODE' in data.columns

    # Each symbol should have 51 datapoints each
    assert data.shape[0] == (LOOKBACK_LEN+1)*data['SYMBOL_CODE'].unique().shape[0]

    assert "DATE_CODE" in data.columns
    assert list(data["DATE_CODE"].sort_values().unique()) == list(range(160-(LOOKBACK_LEN), 161))
    assert list(data["YEAR"].unique()) == [2018, 2017]
    
    
    assert (data.loc[(data["DATE_CODE"]==144) & (data['SYMBOL_CODE']=="ETH"), 
                    ['OPEN', 'HIGH', 'LOW', 'CLOSE']].values.tolist()[0] 
    == [379.69900512695310,395.17098999023440, 377.59298706054690, 386.42498779296875])



def test_transform_data(snowflake_connection, test_assets):
    cur = snowflake_connection
    ti_mock = mock.Mock()


    def mock_xcom_pull(task_ids, key):
        if task_ids == 'Extract_Data_From_Snowflake' and key is None:
            cur.execute("SELECT * FROM TEST_OHLC_WITH_DATES")
            print(cur.fetch_pandas_all())
            return cur.fetch_pandas_all()
        elif task_ids == 'Get_Variables' and key is None:
            return test_assets, "_", "_"
        return None

    # Mock `xcom_pull` with side_effect
    ti_mock.xcom_pull.side_effect = mock_xcom_pull
    result = transform_data(ti=ti_mock)
    
    cols_lower = result.columns.str.lower()
    
    # The result should have length LOOKBACK_LEN + 1
    assert result.shape[0] == LOOKBACK_LEN + 1

    assert is_datetime64_any_dtype(result.index), "column 'DATE' is not type datetime64" 

    test_cols = set(["symbol_code", "currency_code", "range", "exchange_code"])
    assert not test_cols.intersection(set(cols_lower)), "columns 'symbol_code', 'currency_code', 'range', 'exchange_code' are in dataframe"
    

def test_calculate_spread(snowflake_connection, transformed_data, test_close_data):
    cur = snowflake_connection
    ti_mock = mock.Mock()
    window_size=5
    
    def mock_xcom_pull(task_ids, key):
        if task_ids == 'Transform_Snowflake_Data' and key is None:
            return transformed_data
        elif task_ids == 'Get_Variables' and key is None:
            return "_", ["CLOSE_ETH", "CLOSE_BTC"], "_"
        return None

    ti_mock.xcom_pull.side_effect = mock_xcom_pull

    result = calculate_standardized_spread(ti=ti_mock, rolling_win_size=window_size)
    
    new_cols = set(['standardized_spread', 'spread', 'mu', 'std'])
    assert new_cols.intersection(set(result.columns)) == new_cols

    na = result['standardized_spread'][result['standardized_spread'].isna()]
    assert na.shape[0]==window_size
    no_na = result.dropna()
    assert no_na.shape[0] == 151-window_size

    # Test on synthetic data
    test_data = test_close_data
    def mock_xcom_pull(task_ids, key):
        if task_ids == 'Transform_Snowflake_Data' and key is None:
            return test_data
        elif task_ids == 'Get_Variables' and key is None:
            return "_", ["CLOSE_ETH", "CLOSE_BTC"], "_"
        return None
    
    ti_mock.xcom_pull.side_effect = mock_xcom_pull

    result2 = calculate_standardized_spread(ti=ti_mock, rolling_win_size=window_size)
    
    result2 = result2.iloc[-1].round(6)
    assert result2['standardized_spread'] == -1.164986
    assert result2['mu'] == -0.033278
    assert result2['std'] == 1.282847
    assert result2['spread'] == -1.527778
    assert result2.shape[0] == test_data.shape[0]


def test_entropy(transformed_data):
    ti_mock = mock.Mock()
    
    def mock_xcom_pull(task_ids, key):
        if task_ids == 'Engineer_Spread_Features' and key is None:
            return transformed_data
        elif task_ids == 'Get_Variables' and key is None:
            return "_", "_", ["CLOSE_CHANGE_BTC", "CLOSE_CHANGE_ETH"]
        return None
    
    ti_mock.xcom_pull.side_effect = mock_xcom_pull

    result = calculate_entropy(ti=ti_mock)

    assert result.shape[0] == transformed_data.shape[0]
    assert (set(transformed_data.columns).intersection(set(result.columns))
            == set(transformed_data.columns))
    assert [col for col in result.columns if "entropy" in col]


@pytest.mark.skip(reason="Not using")
def test_google_trends(transformed_data):
    ti_mock = mock.Mock()
    search_terms = ['bitcoin', 'ethereum']

    def mock_xcom_pull(task_ids, key):
        if task_ids == 'Engineer_Technical_Features' and key is None:
            return transformed_data
        elif task_ids == 'Get_Symbol_Info' and key is None:
            return search_terms
        return None
    
    ti_mock.xcom_pull.side_effect = mock_xcom_pull

    result = get_google_trends_data(ti=ti_mock)
    
    result_cols_set = set(result.columns)
    assert result.shape[0] == transformed_data.shape[0]
    date_test = result.reset_index()
    assert "DATE" in date_test.columns
    assert (set(transformed_data.columns).intersection(result_cols_set)
            == set(transformed_data.columns))
    assert set(search_terms).intersection(result_cols_set) == set(search_terms)
    


def test_dag_structure(dagbag):
    dag = dagbag.get_dag('feature_engineering')
    task_ids = [task.task_id for task in dag.tasks]
    assert set(task_ids) == {
        'Get_Secrets', 
        'Extract_Data_From_Snowflake', 
        'Create_Feature_Destination_Table',
        'Get_Variables',
        'Transform_Snowflake_Data',
        'Engineer_Spread_Features',
        'Get_Close_Change_Variables',
        'Get_Symbol_Info',
        'Calculate_Entropy_Features',
        'Calculate_Bid_Ask_Spread_Features',
        'Engineer_Cointegration_Stat_Features',
        'Engineer_Technical_Features',
        'Load_Features_to_Snowflake',
        'Trigger_Inference_Job'
        }