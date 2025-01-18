import json

import boto3
import json
import snowflake.connector
import yfinance as yf
import pandas as pd
from datetime import datetime
import json
import pytz

sqs = boto3.client('sqs', region_name='us-west-1')  # replace with your AWS region

UPPER = 0.662016
LOWER = 0.220194

TIMEZONE = pytz.timezone('America/Denver')

BUCKET_NAME = "mybucket1654"

queue_url = 'https://sqs.us-west-1.amazonaws.com/390402566290/FinPredictionQueue'

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
    conn = snowflake.connector.connect(
        user='bobjames',
        password='',
        account="",
        warehouse='COMPUTE_WH',
        database='DATA_ENG_DBT',
        schema=schema,
        role="DBT_EXECUTOR_ROLE"
    )

    cur = conn.cursor()

    return cur, conn


def get_ticker(ticker:str, data_start:str):
    tick_obj = yf.Ticker(ticker)
    df = yf.download(ticker, start=data_start, interval="1d")
    
    try:
        df.columns = df.columns.droplevel(1)
    except Exception:
        pass

    df.columns = df.columns.str.lower()
    df.index.name = df.index.name.lower()

    return df


class Trader:
    def __init__(self, tickers,
                 s3_bucket, 
                 percent_allocation=0.1, 
                 init_account=False,
                 upper_thresh_mp=0.662016,
                 lower_thresh_mp=0.220194
                 ):
        
        # Get data 70 days back
        start = datetime.today() - pd.DateOffset(days=70)
        self.tickers = {symbol: get_ticker(symbol, start) for symbol in tickers}

        self.percent_allocation = percent_allocation
        self.upper_thresh_mp = upper_thresh_mp
        self.lower_thresh_mp = lower_thresh_mp

        self.spread = None
        # calculate spread between ticker 1 and 2
        if len(tickers) == 2:
            self.calculate_spread(tickers[0], tickers[1])

        self.current_prices = {symbol: self.tickers[symbol]['close'].iloc[-1] 
                               for symbol in tickers}
        
        self.bucket = s3_bucket
        self.s3_client = boto3.client('s3')

        account = {"symbol": None, 
                   "amount": 0, 
                   "position_value": 0, 
                   "account_balance": 10000, 
                   "days_in_trade": 0,
                   "trade_open_spread":0, 
                   "date": str(datetime.now().date()),
                   "time": str(datetime.now().time())}
        
        if init_account:
            self.s3_client.put_object(Bucket=self.bucket, 
                                 Key="account.json", 
                                 Body=json.dumps(account))

        self.position = False
        


    def calculate_spread(self, symbol1, symbol2):
        spread = (self.tickers[symbol1]["close"].pct_change() - 
                  self.tickers[symbol2]["close"].pct_change())
        spread = spread.to_frame(name='spread')
        spread['mu'] = spread['spread'].rolling(window=55).mean()
        spread['std'] = spread['spread'].rolling(window=55).std()
        spread["standardized_spread"] = (spread['spread']- spread.mu) / spread['std']
        self.spread = spread


    def load_account(self):
        response = self.s3_client.get_object(Bucket=self.bucket, Key="account.json")
        content = response['Body'].read().decode('utf-8')
        self.account = json.loads(content)
        if self.account['symbol']:
            self.position = True


    def save_account(self):
        self.s3_client.put_object(
            Bucket=self.bucket, 
            Key="account.json", 
            Body=json.dumps(self.account)
        )


    def update_account(self, symbol, amount, cost):
        print("updating_account")
        self.account['account_balance'] = self.account['account_balance'] - cost
        self.account['amount'] = amount
        if symbol is not None:
            self.account['position_value'] = cost
            self.account['trade_open_spread'] = self.spread["standardized_spread"].iloc[-1]
        else:
            self.account['position_value'] = 0
            self.account['trade_open_spread'] = 0
        self.account['symbol'] = symbol
        self.account['date'] = str(datetime.now(TIMEZONE).date())
        self.account['time'] = str(datetime.now(TIMEZONE).time())


    def check_buying_availability(self, cost):
        if cost > self.account['account_balance']:
            print(f"Not enough balance: {self.account['account_balance']}")
            return False
        else:
            return True
        

    def buy(self, symbol, amount):
        price = self.current_prices[symbol]
        cost = price*amount
        
        print(f"Buying {symbol} at price: {price}. Total cost: {cost}")
        if not self.check_buying_availability(cost):
            return False
        else:
            self.update_account(symbol, amount, cost)
            self.position = True
            return True
  

    def sell(self, symbol, amount):
        if self.account['symbol'] is not None and self.account['symbol'] == symbol:
            price = self.current_prices[symbol]
            print(f"SELLING {symbol} at price {price}")
            sell_amount = amount*price
            self.update_account(None, amount=0, cost=-sell_amount)

            # Reset the number of days in a trade
            self.account['days_in_trade'] = 0
            self.position = False

            return True
        return False
    

    def calculate_position_size(self, symbol):
        allocate = self.account['account_balance']*self.percent_allocation
        current_price = self.current_prices[symbol]
        n_shares = allocate / current_price
        return n_shares


    def trade(self, prediction, old_spread):
        self.load_account()

        # Lower and upper standardized_spread barriers
        upper = old_spread*(1 + self.upper_thresh_mp)
        lower = old_spread*(1 - self.lower_thresh_mp)
        
        if self.account['symbol'] is not None:
            current_symbol = self.account['symbol']
            current_spread = self.spread["standardized_spread"].iloc[-1].item()
            
            # Add a day to the holding duration 
            self.account['days_in_trade'] += 1

            # Check for position closing conditions
            if self.account['days_in_trade'] == 2:
                size = self.account['amount']
                self.sell(current_symbol, size)
            if current_spread >= upper or current_spread <= lower:
                size = self.account['amount']
                self.sell(current_symbol, size)
        
        self.save_account()
        self.load_account()
        if self.account['symbol'] is None: # Check if we do not have a position open
            if prediction == 1:
                size = self.calculate_position_size("ETH-USD")
                self.buy("ETH-USD", amount=size)
            elif prediction == 0:
                size = self.calculate_position_size("BTC-USD")
                self.buy("BTC-USD", amount=size)

        self.save_account()
        return self.account



def load_account_to_snowflake(account):
    cur, conn = connect_to_snowflake("STAGING")

    # Load the account info to the position source table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS POSITION_SOURCE (
                DATE DATE,
                TIME TIME,
                SYMBOL VARCHAR(16777216),
                CURRENCY VARCHAR(16777216),
                POSITION_SIZE NUMBER(38,5),
                TRADE_OPEN_SPREAD NUMBER(38,5),
                ACCOUNT_ID VARCHAR(16777216)
    )
        
    """)

    values = (str(account['date']), str(account['time']), account['symbol'], 
              "USD", str(account['amount']), str(account["trade_open_spread"]), 
              "a6gss56y2")
    
    sql = """INSERT INTO POSITION_SOURCE
                VALUES (%s, %s, %s, %s, %s, %s, %s)
          """
    cur.execute(sql, values)

    # Load the account balance info into the account balance source
    values = (account['date'], "a6gss56y2", str(account['account_balance']))
    
    sql2 = f"""INSERT INTO ACCOUNT_BAL_SOURCE
                VALUES (%s, %s, %s)
                """
    cur.execute(sql2, values)



def lambda_handler(event, context):
    # Initialize trader
    tickers = ["BTC-USD", "ETH-USD"]
    trader = Trader(tickers, s3_bucket=BUCKET_NAME)

    try:
        for record in event['Records']:
            # Get the message body
            message_body = record['body']
            
            # Process the message
            print("Processing message:", message_body)
            
            # If the message is JSON, parse it
            try:
                data = json.loads(message_body)
                # Handle message data here (e.g., call a service, write to a database)
                print("Message data:", data)
            except json.JSONDecodeError:
                print("Message is not in JSON format")

            else:
                date = data['date']
                prediction = data['direction'][date]

                spread = data['spread']

                account = trader.trade(prediction, spread)
                print(account)
                load_account_to_snowflake(account)

    except Exception:
        date = event['date']
        prediction = list(event['direction'].keys())[0]
        confidence = list(event['confidence'].keys())[0]
        spread = event['spread']

        account = trader.trade(prediction, spread)
        load_account_to_snowflake(account)
        print(account)

    return {
        'statusCode': 200,
        'body': json.dumps('Sucessfuly traded')
    }
