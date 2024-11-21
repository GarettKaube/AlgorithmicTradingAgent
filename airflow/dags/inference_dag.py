
from datetime import timedelta
import datetime
import time
import boto3

from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

from airflow.utils.dates import days_ago
from airflow import DAG

import pandas as pd

import requests

from scripts.connections import get_secret, connect_to_snowflake
from snowflake.connector.pandas_tools import write_pandas


columns = ['open_eth',
 'high_eth',
 'close_eth',
 'adjusted_close_eth',
 'low_eth',
 'volume_eth',
 'dollar_volume_eth',
 'dollar_volume1m_eth',
 'close_change_eth',
 'close_direction_eth',
 'open_btc',
 'high_btc',
 'close_btc',
 'adjusted_close_btc',
 'low_btc',
 'volume_btc',
 'dollar_volume_btc',
 'dollar_volume1m_btc',
 'close_change_btc',
 'close_direction_btc',
 'mu',
 'std',
 'standardized_spread',
 'close_direction_eth_entropy',
 'close_direction_btc_entropy',
 'bid_ask_spread_close_change_eth',
 'bid_ask_spread_close_change_btc',
 'close_eth_coint',
 'close_btc_coint',
 'ma_5',
 'ma_20',
 'vwap_btc',
 'vwap_eth',
 'ma_upper_co',
 'ma_lower_co',
 'rsi',
 'atr_btc',
 'atr_eth']

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

params = {
    "extract_full_data": False,
    "dest_schema": "FEATURES"
}

def extract_from_snowflake(ti):
     username, password, account_id = ti.xcom_pull(task_ids="Get_Snowflake_Secrets")
     cur, conn = connect_to_snowflake(username, password, account_id,
                                       schema='FEATURES')
     cur.execute(
        "SELECT * FROM FEATURES"
     )

     return cur.fetch_pandas_all()
     



def transform(ti):
     """ Returns the most recent transformed data
     """
     data = ti.xcom_pull(task_ids="Extract_Data_From_Snowflake")

     data.columns = data.columns.str.lower()
     
     # Make sure these columns are floats
     for col in data.columns:
         if data[col].dtype != float and col != "date":
            data[col] = data[col].astype(float)

     data['ma_upper_co'] = data['ma_upper_co'].astype(int)
     data['ma_lower_co'] = data['ma_lower_co'].astype(int)
     
     date = data['date'].iloc[-1]

     data = data.set_index("date")
     data.index = pd.to_datetime(data.index)
     
     try:
        data = data.rename({"ethereum": "etherium"},axis=1)
     except Exception:
        pass

     data = data[columns]
     
     try:
        X =  data.drop(['spread'], axis=1)
     except Exception:
        X = data

     mu = X['mu'].iloc[-1]
     std_ = X['std'].iloc[-1]
     spread = X['standardized_spread'].iloc[-1]
     return X.iloc[-1].to_list(), mu, std_, spread, date


def get_ecs_task_arn(ti):
    cluster_name = ti.xcom_pull(task_ids="Get_ECS_Cluster_Name")[0]

    ecs_client = boto3.client('ecs', region_name='us-west-1')
    task_arns_response = ecs_client.list_tasks(cluster=cluster_name)

    task_arns = task_arns_response['taskArns']

    # Step 2: Describe the tasks if there are any
    if task_arns:
        tasks_response = ecs_client.describe_tasks(cluster=cluster_name, tasks=task_arns)
        tasks = tasks_response['tasks']

    if len(tasks) != 1:
        print("Too many tasks")
        return None
    

    return tasks[0]['taskArn']


def get_inference_server_ip(ti):
    cluster_name = ti.xcom_pull(task_ids="Get_ECS_Cluster_Name")[0]
    task_arn = ti.xcom_pull(task_ids="Get_ECS_Task_ARN")

    public_ip = None

    ec2_client = boto3.client('ec2', region_name='us-west-1')
    ecs_client = boto3.client('ecs', region_name='us-west-1')

    task_response = ecs_client.describe_tasks(cluster=cluster_name, tasks=[task_arn])

    # Extract the ENI ID
    eni_id = None
    if task_response['tasks']:
        attachments = task_response['tasks'][0].get('attachments', [])
        for attachment in attachments:
            if attachment['type'] == 'ElasticNetworkInterface':
                for detail in attachment['details']:
                    if detail['name'] == 'networkInterfaceId':
                        eni_id = detail['value']
                        break
    if eni_id:
        eni_response = ec2_client.describe_network_interfaces(NetworkInterfaceIds=[eni_id])
        public_ip = eni_response['NetworkInterfaces'][0]\
            .get('Association', {})\
            .get('PublicIp')
    
    return public_ip


def get_token(ti):
    ip = ti.xcom_pull(task_ids="Retreive_Inference_Server_IP")
    username, password  = ti.xcom_pull(task_ids="Get_inference_server_oath_token_secrets")

    token_url = f"http://{ip}:8000/token"
    print(token_url)

    data = { 
        "username": username,
        "password": password
    }
    
    response = requests.post(token_url, data=data)

    # Check if the request was successful
    if response.status_code == 200:
        token = response.json().get("access_token")
        return token


def get_model_predictions(ti):
    X, _, _, _, date = ti.xcom_pull(task_ids="Transform_Data")
    token = ti.xcom_pull(task_ids="Get_Token")
    ip = ti.xcom_pull(task_ids="Retreive_Inference_Server_IP")

    url = f"http://{ip}:8000/forecast"

    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    body = {
        "model_name": "xgboost_btc_eth_spread",
        "date": [str(date)],
        "features": columns,
        "X": [X]
    }

    response = requests.post(url, headers=headers, json=body)

    if response.status_code == 200:
        print(response)
        return response.json()
    

def send_prediction_to_sqs(ti):
    """Send the predictions to AWS SQS
    """
    import json
    X, mu, std_, spread, date = ti.xcom_pull(task_ids="Transform_Data")
    response_json = ti.xcom_pull(task_ids="Generate_Model_Predictions")
    
    try:
        mu = mu.item()
        std_ = std_.item()
        spread = spread.item()
    except Exception as e:
        print(e)

    prediction_json = response_json['Forecast']
    prediction_json['date'] = date
    prediction_json['mu'] = mu
    prediction_json['std'] = std_
    prediction_json['spread'] = spread

    sqs = boto3.client('sqs', region_name='us-west-1') 
    url = 'https://sqs.us-west-1.amazonaws.com/390402566290/FinPredictionQueue'


    response = sqs.send_message(
        QueueUrl=url,
        MessageBody=json.dumps(prediction_json),
        MessageAttributes={
            'AttributeKey': {
                'StringValue': 'AttributeValue',
                'DataType': 'String'
            }
        }
    )
    print("Message ID:", response['MessageId'])
    # Give the trader lambda function time to execute 
    # before starting next job   
    time.sleep(10)
    


def load_prediction_to_snowflake(ti):
     username, password, account_id = ti.xcom_pull(task_ids="Get_Snowflake_Secrets")
     X, _, _, _, date = ti.xcom_pull(task_ids="Transform_Data")
     
     response_json = ti.xcom_pull(task_ids="Generate_Model_Predictions")
     prediction_json = response_json['Forecast']
     
     model_id = response_json['request']['model_name']

     prediction = list(prediction_json['direction'].values())[0]
     confidence = list(prediction_json["confidence"].values())[0]

     cur1, conn1 = connect_to_snowflake(username, password, account_id,
                                       schema='STAGING')
     
     cur1.execute("SELECT MAX(DATE) FROM PREDICTION_SOURCE")
     latest_date = cur1.fetch_pandas_all()["MAX(DATE)"].item()

     print(date, latest_date)

     if date == latest_date:
         cur1.execute(
             f"UPDATE PREDICTION_SOURCE SET PREDICTION={prediction} WHERE DATE={date}"
         )
         cur1.execute(
             f"UPDATE PREDICTION_SOURCE SET CONFIDENCE={confidence} WHERE DATE={date}"
         )
     else:
         cur1.execute(f"""INSERT INTO PREDICTION_SOURCE 
                      VALUES (%s, %s, %s, %s, %s)
                      """, (date, prediction, confidence, None, model_id))

     # Clean up features table
     cur, conn = connect_to_snowflake(username, password, account_id,
                                       schema='FEATURES')
     cur.execute("DROP TABLE FEATURES;")


with DAG(
    dag_id="inference",
    description="",
    default_args=args,
    catchup=False,
    start_date=datetime.datetime(2024, 5, 16),
    schedule=None,
    is_paused_upon_creation=False,
) as inference_dag:
    
    username, password, account_id = get_secret("snowflake_connection")

    sec = PythonOperator(
        task_id="Get_Snowflake_Secrets",
        python_callable=get_secret,
        op_kwargs={"secret_name": "snowflake_connection"}
    )

    clust_name = PythonOperator(
        task_id="Get_ECS_Cluster_Name",
        python_callable=get_secret,
        op_kwargs={"secret_name": "ecs_cluster_name"}
    )

    inf_sec = PythonOperator(
        task_id="Get_inference_server_oath_token_secrets",
        python_callable=get_secret,
        op_kwargs={"secret_name": "inference_server_oath_token_secrets"}
    )

    task_arn = PythonOperator(
        task_id="Get_ECS_Task_ARN",
        python_callable=get_ecs_task_arn
    )

    inf_ip = PythonOperator(
        task_id="Retreive_Inference_Server_IP",
        python_callable=get_inference_server_ip
    )

    token = PythonOperator(
        task_id="Get_Token",
        python_callable=get_token
    )

    extract = PythonOperator(
        task_id="Extract_Data_From_Snowflake",
        python_callable=extract_from_snowflake,
    )

    transform_ = PythonOperator(
        task_id="Transform_Data",
        python_callable=transform
    )

    predict = PythonOperator(
        task_id="Generate_Model_Predictions",
        python_callable=get_model_predictions
    )

    load_pred = PythonOperator(
        task_id="Load_Prediction_to_Snowflake",
        python_callable=load_prediction_to_snowflake
    )

    sqs = PythonOperator(
        task_id="Send_Prediction_to_SQS",
        python_callable=send_prediction_to_sqs
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command = f"dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --vars '{{\"DBT_USER\": \"{username}\", \"DBT_PASSWORD\": \"{password}\", \"DBT_HOST\": \"{account_id}\"}}'"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command = f"dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --vars '{{\"DBT_USER\": \"{username}\", \"DBT_PASSWORD\": \"{password}\", \"DBT_HOST\": \"{account_id}\"}}'"
    )

sec >> extract >> transform_ >> predict
clust_name >> task_arn >> inf_ip >> token >> predict >> load_pred
inf_sec >> token >> predict >> sqs >> dbt_run >> dbt_test
