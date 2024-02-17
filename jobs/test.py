from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import snowflake.connector
import json
import pymongo
import pandas as pd
import pytz

def migrate_data_to_snowflake():
    print('inside')

dag = DAG('Test_Dag', description='test Dag', start_date=datetime(2024, 2, 14), catchup=False)

hello_operator = PythonOperator(task_id='hello_task1', python_callable=migrate_data_to_snowflake, dag=dag)

hello_operator
