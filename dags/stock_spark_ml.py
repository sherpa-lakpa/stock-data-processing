from datetime import timedelta, datetime
import os
import pickle
import random
import pandas as pd
from pyspark.sql.functions import lit
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import element_at, split, col


from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',    
    'start_date': days_ago(1)
}

today = datetime.today().strftime('%Y%m%d')
stage_path = "/usr/local/spark/staging/"

spark_dag = DAG(
        dag_id = "stock_spark_airflow",
        default_args=default_args,
        schedule_interval=None,	
        dagrun_timeout=timedelta(minutes=60*24),
        description='use case of sparkoperator in airflow',
)


def verify_raw_data_path_func():
    output_path = stage_path + today + "/raw_data_processing"

    # Check whether the specified path exists or not
    if not os.path.exists(output_path):
        # Create a new directory because it does not exist
        os.makedirs(output_path)
        print("The new directory is created: ", output_path)

def verify_feature_data_path_func():
    output_path = stage_path + today + "/feature_engineering"
    
    # Check whether the specified path exists or not
    if not os.path.exists(output_path):
        # Create a new directory because it does not exist
        os.makedirs(output_path)
        print("The new directory is created!")

with spark_dag:
    with TaskGroup("raw_data_processing", tooltip="Tasks for raw_data_processing") as task_group_raw_data_processing:
        verify_raw_data_path = PythonOperator(
            task_id="verify_raw_data_path", 
            python_callable = verify_raw_data_path_func)

        stock_data_processing = SparkSubmitOperator(
            application = "/usr/local/spark/app/raw_data_processing.py",
            conn_id= 'spark_local', 
            task_id='stock_data_processing', 
            application_args=['stocks'],
		)

        verify_raw_data_path >> stock_data_processing
        
        etf_data_processing = SparkSubmitOperator(
            application = "/usr/local/spark/app/raw_data_processing.py",
            conn_id= 'spark_local',
            task_id='etf_data_processing', 
            application_args=['etfs'],
		)
        
        verify_raw_data_path >> etf_data_processing

    with TaskGroup("feature_engineering", tooltip="Tasks for feature_engineering") as task_group_feature_engineering:
        task_verify_feature_data_path = PythonOperator(
            task_id='verify_feature_data_path',
            python_callable = verify_feature_data_path_func
        )

        stock_feature_engineering = SparkSubmitOperator(
            application = "/usr/local/spark/app/feature_engineering_processing.py",
            conn_id= 'spark_local', 
            task_id='stock_feature_processing', 
            application_args=['stocks'],
		)

        etf_feature_engineering = SparkSubmitOperator(
            application = "/usr/local/spark/app/feature_engineering_processing.py",
            conn_id= 'spark_local',
            task_id='etf_feature_processing', 
            application_args=['etfs'],
		)

        task_verify_feature_data_path >> [stock_feature_engineering, etf_feature_engineering]

    train_model = SparkSubmitOperator(
            application = "/usr/local/spark/app/train_model.py",
            conn_id= 'spark_local',
            task_id='train_model', 
		)
    
    task_group_raw_data_processing >> task_group_feature_engineering >> train_model
