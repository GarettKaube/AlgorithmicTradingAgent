import sys 
import os
from pathlib import Path
import pytest
from unittest.mock import patch, Mock, MagicMock
import datetime
import pandas as pd
import numpy as np

src_path = Path(os.getcwd()) / Path("src")
sys.path.insert(0, str(src_path))

from trading_service.paper_trading_aws_lambda import Trader
np.random.seed(42)

@pytest.fixture
def tickers():
    return ["BTC-USD", 'ETH-USD']


@pytest.fixture
def start_account():
    account = {"symbol": None, 
                   "amount": 0, 
                   "position_value": 0, 
                   "account_balance": 10000, 
                   "days_in_trade": 0,
                   "trade_open_spread":0, 
                   "date": str(datetime.datetime.now().date()),
                   "time": str(datetime.datetime.now().time())}
    return account


@pytest.fixture
def test_data():
    length = 5
    dates = pd.date_range(start="2019-12-20", periods=5, freq='1d')
    data = {
        'close': np.random.randn(length),
        'standardized_spread': np.random.randn(length),
        'low': np.random.randn(length),
        'high': np.random.randn(length),
        'adj close': np.random.randn(length),
        'volume': np.random.randn(length)
    }
    
    df = pd.DataFrame(data, index=dates)
    df.index.name='date'
    return df


def test_buy(test_data, start_account):
    tickers = ["BTC-USD", 'ETH-USD']
    today = "2019-12-20"
    spread = test_data[['standardized_spread']]

    global account
    account = start_account
    print("start account", account)
    
    with (patch('boto3.client') as mock_boto3, 
          patch('trading_service.paper_trading_aws_lambda.datetime') as mock_datetime,
          ):
        mock_datetime.today.return_value = pd.to_datetime(today) + pd.DateOffset(days=5)
        mock_datetime.now.return_value.date.return_value = today
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        mock_boto3.return_value = None
        trader = Trader(tickers, s3_bucket=None, init_account=False)

        # Make sure prices start at today
        trader.current_prices = {symbol: trader.tickers[symbol]['close'].loc[today]
                               for symbol in tickers}
        
        print(trader.current_prices['BTC-USD'])
        trader.calculate_spread = Mock()
        trader.load_account = Mock()
        trader.save_account = Mock()

        def mock_spread_se():
            trader.spread = spread.loc[[today]]
        
        def mock_load_account_se():
            trader.account = account

        def mock_save_account_se():
            global account
            account = trader.account
            

        # "Calculate" the standardized spread
        trader.calculate_spread.side_effect = mock_spread_se
        trader.load_account.side_effect = mock_load_account_se
        trader.save_account.side_effect = mock_save_account_se

        trader.calculate_spread()
        trader.load_account()
        trader.save_account()
        # Execute trades
        trader.trade(1, spread.loc[today])
        
        assert account['position_value'] == 1000.0
        assert account['account_balance'] == 9000.0
        assert account['days_in_trade'] == 0
        assert account['trade_open_spread'] == spread.iloc[0].item()

        # Trade for second day
        trader = Trader(tickers, s3_bucket=None, init_account=False)
        next_day = pd.to_datetime(today) + pd.DateOffset(days=1)

        def mock_spread_se():
            trader.spread = spread.loc[[next_day]]

        def mock_load_account_se():
            trader.account = account

        # Update current prices to next day
        trader.current_prices = {symbol: trader.tickers[symbol]['close'].loc[next_day]
                               for symbol in tickers}
        
        trader.calculate_spread = Mock()
        trader.load_account = Mock()
        trader.save_account = Mock()

        trader.calculate_spread.side_effect = mock_spread_se
        trader.load_account.side_effect = mock_load_account_se
        trader.save_account.side_effect = mock_save_account_se
        
        trader.calculate_spread()
        trader.load_account()

        assert account['position_value'] == 1000.0
        assert account['account_balance'] == 9000.0
        assert account['days_in_trade'] == 0
        assert account['symbol'] == 'ETH-USD'
        assert account['trade_open_spread'] == spread.iloc[0].item()
        
        trader.trade(1, spread.loc[today].item())

        print(trader.account)
        # Expect a sell
        assert round(account['account_balance'], 3) == round(
            10000.0 + (128.1310 - 129.0661) * 7.747970585234475, 3)
        
        assert account['symbol'] is None
        assert account['position_value'] == 0.0
        assert account['days_in_trade'] == 0

