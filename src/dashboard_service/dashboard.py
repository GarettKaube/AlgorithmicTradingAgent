import streamlit as st 
import boto3
import sys
import pandas as pd
sys.path.insert(0, "./src")
import matplotlib.pyplot as plt 
from utils.utils import connect_to_snowflake, get_ticker
import datetime
import plotly.graph_objects as go

import plotly.express as px

import time

plot_background_color = "#3c3434"

mlflow_url = "http://127.0.0.1:5000/health"

st.set_page_config(layout="wide")

# def extract_from_snowflake(n_days=150):
#      """ Extracts the latest n_days data from the ASSETS data mart
#      """
     
#      cur, conn = connect_to_snowflake(schema='PROD_MART_ASSETS')
     
#      # n_days==0 -> extract all data
#      join = "DIM_DATE" if n_days == 0 else "LATEST_DATES"

#      # Query to find the date_codes for the last 150 days
#      # And join the OHLC data with those days to get the
#      # latest 150 OHLC data
#      # Note: the DATE_CODE column will be duplicated 
#      cur.execute(f"""
#                  WITH LATEST_DATES AS (
#                     SELECT DISTINCT
#                         ohlc.DATE_CODE,
#                         YEAR,
#                         MONTH,
#                         DAY
#                     FROM FACT_OHLC as ohlc
#                     JOIN DIM_DATE as dates ON
#                         ohlc.DATE_CODE = dates.DATE_CODE
#                     WHERE ohlc.DATE_CODE >= (
#                         SELECT MAX(DATE_CODE) FROM FACT_OHLC
#                         ) - {n_days}
#                 )

#                 SELECT 
#                     ohlc.OPEN,
#                     ohlc.HIGH,
#                     ohlc.CLOSE,
#                     ohlc.ADJUSTED_CLOSE,
#                     ohlc.LOW,
#                     ohlc.VOLUME,
#                     ohlc.SYMBOL_CODE,
#                     ohlc.EXCHANGE_CODE,
#                     ohlc.CURRENCY_CODE,
#                     ohlc.DATE_CODE,
#                     ohlc.DOLLAR_VOLUME,
#                     ohlc.DOLLAR_VOLUME1M,
#                     ohlc.RANGE,
#                     ohlc.TRUE_RANGE,
#                     ohlc.CLOSE_CHANGE,
#                     ohlc.CLOSE_DIRECTION,
#                     dates.YEAR,
#                     dates.MONTH,
#                     dates.DAY
#                 FROM FACT_OHLC as ohlc
#                 JOIN {join} as dates ON
#                     ohlc.DATE_CODE = dates.DATE_CODE;
#                  """
#                 )
     
#      data = cur.fetch_pandas_all()

#      cur.close()
#      conn.close()
#      return data


def get_mlflow_server_health():
    import requests

    # Send a GET request to check the server health
    try:
        response = requests.get(mlflow_url, timeout=5)
        print(response)
        if response.status_code == 200:
            return True
        else:
            return False
    except requests.exceptions.ConnectTimeout:
        return False
    except requests.exceptions.ReadTimeout:
        return False


def get_task_arns():
    cluster_name = "modelCluster"

    ecs_client = boto3.client('ecs', region_name='us-west-1')
    task_arns_response = ecs_client.list_tasks(cluster=cluster_name)

    task_arns = task_arns_response['taskArns']

    if task_arns:
        return True
    return False


def calculate_spread(symbol1:pd.DataFrame, symbol2:pd.DataFrame):
    spread = symbol1["close"].pct_change() - symbol2["close"].pct_change()
    spread = spread.to_frame(name='spread')
    spread['mu'] = spread['spread'].rolling(window=55).mean()
    spread['std'] = spread['spread'].rolling(window=55).std()
    spread["standardized_spread"] = (spread['spread']- spread.mu) / spread['std']
    return spread["standardized_spread"]


def create_date_col(data):
    data = data.copy()
    data['DATE'] = data[["YEAR", "MONTH", "DAY"]]\
        .apply(lambda x: pd.Timestamp(x["YEAR"], x["MONTH"], x["DAY"]), axis=1)
    return data


def get_latest_prediction():
    cur, conn = connect_to_snowflake(schema='PROD_MART_ASSETS')
    cur.execute("""SELECT pred.*, date.YEAR, date.MONTH, date.DAY
                FROM PROD_MART_ASSETS.FACT_PREDICTIONS as pred
                JOIN DIM_DATE as date
                    on pred.DATE_CODE = date.DATE_CODE
                """)

    predictions = cur.fetch_pandas_all()
    predictions = predictions.drop(["DATE_CODE"],axis=1)
    predictions = create_date_col(predictions)
    predictions = predictions.set_index("DATE")
    return predictions.iloc[-1]


def get_position():
    cur, conn = connect_to_snowflake(schema='PROD_MART_ASSETS')
    cur.execute("""SELECT 
                pos.SYMBOL_CODE, 
                pos.CURRENCY_CODE, 
                pos.POSITION_SIZE,
                pos.TRADE_OPEN_SPREAD, 
                pos.ACCOUNT_ID
                , date.YEAR, date.MONTH, date.DAY
                FROM PROD_MART_ASSETS.FACT_POSITION as pos
                JOIN DIM_DATE as date
                    on pos.DATE_CODE = date.DATE_CODE
                """)
    
    postitions = cur.fetch_pandas_all()
    postitions = create_date_col(postitions)
    postitions = postitions.set_index("DATE")
    postitions = postitions.drop(["YEAR", "MONTH", "DAY"],axis=1)
    print(postitions)
    return postitions.iloc[-1]


def plot_asset(data, symbol_name, y_axis_title="Close Price", 
               spread=None, position_start=None):
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
        x=data.index.astype(str),
        y=data,
        name = symbol_name)
    )

    fig.update_layout(yaxis_title=y_axis_title, title={
            'text' : symbol_name,
            'x':0.47,
            'xanchor': 'center'
        },
        paper_bgcolor=plot_background_color,
        plot_bgcolor=plot_background_color,
        )
    
    if spread is not None:
        upper = spread*(1+0.662016)
        lower = spread*(1-0.220194)
        fig.add_hline(y=lower, line_dash="dash", 
                      line_color="red", annotation_text="Lower Barrier")
        fig.add_hline(y=upper, line_dash="dash", 
                      line_color="green", annotation_text="Upper Barrier")
    
    if position_start is not None:
        fig.add_shape(
            type="line",
            x0=str(position_start), x1=str(position_start), y0=0, y1=1, 
            yref="paper",  
            line=dict(color="blue", width=2, dash="dot")
        )
        fig.add_annotation(
            x=str(position_start), y=0.9,
            xref="x", yref="paper",  
            text="Position open",  
            showarrow=True,  
            arrowhead=2,
            ax=0, ay=-40 
        )
        fig.update_layout(height=800)
    
    fig.update_xaxes(gridcolor="gray")
    fig.update_yaxes(gridcolor="gray")

    return fig

cols = st.columns(4)

with cols[0]:
    task = st.empty()
with cols[1]:
    mlflow = st.empty()

with cols[2]:
    pred = st.empty()
st.divider()

st.header("Current Position")
pos = st.empty()

st.divider()

st.header("Standardized Spread BTC-ETH")
sp = st.empty()
st.divider()
st.header("Bitcoin Close Price")
ts1 = st.empty()
st.divider()
st.header("Ethereum Close Price")
ts2 = st.empty()


i=0
while True:
    tasks = get_task_arns()
    ml_health = get_mlflow_server_health()
    
    if_label = "Online" if tasks else "Offline"
    ml_label = "Online" if ml_health else "Offline"
    task.metric("Inference Server Status", if_label)
    mlflow.metric("MLFlow Server Status", ml_label)

    # Get and display last prediction
    prediction = get_latest_prediction()
    pred.metric("Current Prediction", 
                value=f"{prediction['PREDICTION']}({prediction['CONFIDENCE']:.2f})")

    # Get and display current possition
    position = get_position()
    pos.dataframe(data=position.to_frame().T)

    # Download bitcoin and ethereum data
    data_start = datetime.datetime.now() - pd.DateOffset(days=100)
    btc = get_ticker("BTC-USD", data_start=data_start, interval="1d")
    eth = get_ticker("ETH-USD", data_start=data_start, interval="1d")
    
    # Plot bitcoin close prices
    btc_fig = plot_asset(btc['close'], symbol_name="BTC")
    ts1.plotly_chart(btc_fig, key=f"btcplot_{i}")

    # Plot Ethereum prices
    eth_fig = plot_asset(eth['close'], symbol_name="ETH")
    ts2.plotly_chart(eth_fig, key=f"ethplot_{i}")

    # Plot spread, the barriers, and the posistion opening
    spread = calculate_spread(btc, eth)
    spread_fig = plot_asset(
        spread.dropna(), 
        symbol_name="Z-SPREAD", 
        y_axis_title="Standardized Spread BTC-ETH", 
        spread=position["TRADE_OPEN_SPREAD"].item(), 
        position_start=position.name
    )
    sp.plotly_chart(spread_fig, key=f"spreadplot_{i}")

    time.sleep(10)
    i+=1





