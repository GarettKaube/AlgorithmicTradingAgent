import pandas as pd
from datetime import timedelta
import datetime

from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.models.param import Param
from distutils.util import strtobool


from feature_eng_pipeline.extract import extract_from_snowflake, get_symbol_info
from feature_eng_pipeline.transform import (
    transform_data, calculate_standardized_spread, 
    calculate_entropy, get_bid_ask_spread,
    get_coint_stats, compute_technical_features, 
    get_google_trends_data
)
from feature_eng_pipeline.load import create_features_schema_and_table, load_features_to_snowflake
from feature_eng_pipeline.get_variables import get_variables, get_close_chg_vars
from scripts.connections import get_secret

args = {
    'owner': "Garett",
    'depends_on_past': False,
    "start_date": days_ago(31),
    "email": "gkaube@outlook.com",
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

params = {
    "extract_full_data": Param("False"),
    "dest_schema": "FEATURES"
}

with DAG(
    dag_id="feature_engineering",
    description="",
    default_args=args,
    catchup=False,
    start_date=datetime.datetime(2024, 5, 16),
    schedule=None,
    is_paused_upon_creation=False,
    params=params,
) as feature_engineering_dag:
    
    extract_full_data = feature_engineering_dag.params["extract_full_data"]


    n_days = 150
    print(n_days)

    dest_schema = feature_engineering_dag.params["dest_schema"]
    
    sec = PythonOperator(
        task_id="Get_Secrets",
        python_callable=get_secret,
        op_kwargs={"secret_name": "snowflake_connection"}
    )

    extract = PythonOperator(
        task_id="Extract_Data_From_Snowflake",
        python_callable=extract_from_snowflake,
        op_kwargs={"n_days": n_days}
    )

    dest = PythonOperator(
        task_id="Create_Feature_Destination_Table",
        python_callable=create_features_schema_and_table,
        op_kwargs={"dest_schema": dest_schema}
    )

    get_vars = PythonOperator(
        task_id="Get_Variables",
        python_callable=get_variables
    )

    transform_sf = PythonOperator(
        task_id="Transform_Snowflake_Data",
        python_callable=transform_data
    )

    spread_calc = PythonOperator(
        task_id="Engineer_Spread_Features",
        python_callable=calculate_standardized_spread
    )

    close_chng_vars = PythonOperator(
        task_id = "Get_Close_Change_Variables",
        python_callable=get_close_chg_vars
    )

    symbol_info = PythonOperator(
        task_id="Get_Symbol_Info",
        python_callable=get_symbol_info,
    )

    entropy_ = PythonOperator(
        task_id="Calculate_Entropy_Features",
        python_callable=calculate_entropy
    )

    bas = PythonOperator(
        task_id="Calculate_Bid_Ask_Spread_Features",
        python_callable=get_bid_ask_spread
    )

    ci = PythonOperator(
        task_id="Engineer_Cointegration_Stat_Features",
        python_callable=get_coint_stats
    )

    tf = PythonOperator(
        task_id="Engineer_Technical_Features",
        python_callable=compute_technical_features
    )

    # gt = PythonOperator(
    #     task_id="Get_Google_Trends_Data",
    #     python_callable=get_google_trends_data
    # )

    load = PythonOperator(
        task_id="Load_Features_to_Snowflake",
        python_callable=load_features_to_snowflake,
        op_kwargs={"dest_schema": dest_schema}
    )

    inference = TriggerDagRunOperator(
        task_id="Trigger_Inference_Job",
        trigger_dag_id="inference"
    )


sec >> extract >> get_vars >> transform_sf >> [spread_calc, close_chng_vars]>> entropy_ >> bas >> ci >> tf >> load >> inference
#sec >> symbol_info >> gt
sec >> dest >> load

