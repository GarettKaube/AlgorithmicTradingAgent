import pytest
import datetime
import yfinance as yf
import pandas as pd
from unittest.mock import patch, Mock, MagicMock
import random
import numpy as np
import sys
sys.path.insert(0, "./airflow/dags")
from ingestion_dag import get_ticker, extract_data_from_source, check_upload

# random date 
today = "2019-12-31"
np.random.seed(42)

@pytest.fixture
def eth_data_01():
    return yf.download("ETH-USD", start="2019-12-01", 
                       interval="1d", 
                       end="2020-01-01")

@pytest.fixture
def eth_data_31():
    return yf.download("ETH-USD", start="2019-12-31", 
                       interval="1d", 
                       end="2020-01-01")

@pytest.fixture
def test_ticker_data_eth():
    length = 12
    dates = pd.date_range(start="2019-12-20", end="2019-12-31")
    data = {
        'close': np.random.randn(length),
        'open': np.random.randn(length),
        'low': np.random.randn(length),
        'high': np.random.randn(length),
        'adj close': np.random.randn(length),
        'volume': np.random.randn(length)
    }
    
    df = pd.DataFrame(data, index=dates)
    df.index.name='date'
    return df


@pytest.fixture
def test_ticker_data_btc():
    length = 12
    dates = pd.date_range(start="2019-12-20", end="2019-12-31")
    data = {
        'close': np.random.uniform(size=length),
        'open': np.random.uniform(size=length),
        'low': np.random.uniform(size=length),
        'high': np.random.uniform(size=length),
        'adj close': np.random.uniform(size=length),
        'volume': np.random.uniform(size=length)
    }
    
    df = pd.DataFrame(data, index=dates)
    df.index.name='date'
    return df


@pytest.fixture
def tickers():
    return {
        "BTC" : "BTC-USD",
        "ETH": "ETH-USD",
        # CODE: CODE-CURRENCY
    }

@pytest.fixture
def test_extracted_asset_source(test_ticker_data_btc, test_ticker_data_eth):
    btc = test_ticker_data_btc
    eth = test_ticker_data_eth

    btc['symbol'] = "BTC"
    eth['symbol'] = 'ETH'

    btc['currency'] = "USD"
    eth['currency'] = "USD"

    btc['exchange'] = "CCC"
    eth['exchange'] = "CCC"

    data = pd.concat([btc, eth], axis=0).reset_index()
    data["actual"] = np.nan
    data['prediction'] = np.nan
    data['model_id'] = np.nan
    assert data.shape[0] == 24

    return data



@pytest.fixture
def test_database_asset_source(test_extracted_asset_source):
    """ Mimic a snowflake table
    """
    data = test_extracted_asset_source
    data = data[data['date'] <= pd.to_datetime("2019-12-25")]
    data.columns = data.columns.str.upper()
    data.columns = data.columns.str.replace(" ", "_")
    
    data['DATE'] = data['DATE'].astype('str')
    return data



@pytest.mark.parametrize("data_start", [("2019-12-01"), ("2019-12-31")])
def test_get_ticker(data_start, eth_data_01, eth_data_31):
    from datetime import date

    def mock_yf(ticker, start, **kwargs):
        if start == "2019-12-01":
            return eth_data_01
        elif start == "2019-12-31":
            return eth_data_31
        return None

    with patch('datetime.date') as mock_today, patch('yfinance.download') as mock_yf_download:
        mock_today.today.return_value = pd.to_datetime(today)
        mock_today.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        mock_yf_download.side_effect = mock_yf
        
        raw, _ = get_ticker("ETH-USD", data_start=data_start)
        
        if data_start == "2019-12-01":
            assert data_start not in list(raw.index)
            assert all([pd.to_datetime(i) > pd.to_datetime(data_start) 
                        for i in list(raw.index)])
        elif data_start == "2019-12-31":
            print("raw__:", raw.index)
            assert pd.to_datetime(data_start) in list(raw.index)
            assert raw.shape[0] == 1


@pytest.mark.parametrize("data_start", [("2019-12-01"), ("2019-12-31")])
def test_extract_data_from_source(
    test_ticker_data_eth,
    test_ticker_data_btc,
    tickers,
    data_start
    ):
    ti_mock = Mock()

    ti_mock.xcom_pull.return_value = "2019-12-20"

    def mock_get_ticker_se(code_cur, data_start):
        if code_cur == "BTC-USD":
            return test_ticker_data_btc, yf.Ticker("BTC-USD")
        elif code_cur == "ETH-USD":
            return test_ticker_data_eth, yf.Ticker("ETH-USD")
        return None
    
    with patch("ingestion_dag.get_ticker") as mock_get_ticker:
        mock_get_ticker.side_effect = mock_get_ticker_se
        result = extract_data_from_source(ti=ti_mock, tickers=tickers)
        
        assert len(result.columns) == 6 + 6
        assert 'date' in result.columns
        assert result.shape[0] == 12*2
        assert all([result[col].dtype == float 
                    for col in ['open', 'close', 'high', 'low']])
        

@pytest.mark.parametrize("current_date", [("2019-12-25"), "2019-12-31"])
def test_check_if_upload(current_date, test_database_asset_source, test_extracted_asset_source):
    global database
    database = test_database_asset_source

    source = test_extracted_asset_source.copy()
    source = source[(source['date']<=pd.to_datetime(current_date))
                    & (source['date']>pd.to_datetime(database['DATE'].max()))]

    def ti_mock_se(task_ids):
        if task_ids == "Get_Recent_Date":
            return pd.to_datetime(current_date)
        elif task_ids == "Extract_From_Source":
            return source
        elif task_ids == "Get_Snowflake_Secrets":
            return "None", "None", "None"
    
    ti_mock = Mock()
    ti_mock.xcom_pull.side_effect = ti_mock_se
        
    
    def load_se(data, conn, dest_table):
        global database
        
        database = pd.concat([database, source], axis=0).reset_index(drop=True)
        database=database.sort_values(by=['DATE', 'SYMBOL'])
        return True, '_', '_', '_'

        
    with (patch('ingestion_dag.connect_to_snowflake') as cts_mock, 
        patch('datetime.date') as dt_mock, 
        patch('ingestion_dag.write_pandas') as write_pandas_mock):

        # Mock the datetime.date.today
        dt_mock.today.return_value = pd.to_datetime(current_date).date()
        write_pandas_mock.side_effect = load_se

        mocked_cur = MagicMock()
        mocked_conn = MagicMock()

        cts_mock.return_value = (mocked_cur, mocked_conn)

        # Mock cur.execute and cur.fetch_pandas_all
        mocked_cur.execute.return_value = None
        mocked_cur.fetch_pandas_all.return_value = database['DATE']

        # Function we are testing
        check_upload(ti=ti_mock, dest_table=None)

        if current_date == "2019-12-25":
            write_pandas_mock.assert_not_called, 'write_pandas shouldnt be called'
        else:    
            s = source.sort_values(by=list(source.columns)).reset_index(drop=True)
            d = database.sort_values(by=list(database.columns)).reset_index(drop=True)

            db_size = database.shape[0]
            source_size = s.shape[0]

            for i in range(1, source_size):
                row = s.iloc[source_size-i]
                db_row = d.iloc[db_size-i]
                assert db_row.equals(row)
        
        assert (database['DATE'].value_counts()==2).all()
        
