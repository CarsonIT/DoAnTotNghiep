import time
import requests 
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.sensors.python import PythonSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import json
import csv, urllib.request
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator

def connection_des():
    conn_des = BaseHook.get_connection('DestinationCovid')
    engine_des = create_engine(f'postgresql://{conn_des.login}:{conn_des.password}@{conn_des.host}:{conn_des.port}/{conn_des.schema}')
    return engine_des

def connection_stage():
    conn_stage = BaseHook.get_connection('StageCovid')
    engine_stage = create_engine(f'postgresql://{conn_stage.login}:{conn_stage.password}@{conn_stage.host}:{conn_stage.port}/{conn_stage.schema}')
    return engine_stage
#Here we will create python functions which are tasks these functions can call by a python operator. The below code is that we request the JSON data from the URL and write JSON data into CSV file format.
# The below function will connect to Postgres data by specifying the credentials and creating the driver_data table in the specified database, then will read the stored CSV file row by row and insert it into the table.

def check_condition(**context):
    engine_des = connection_des()

    ###Load into 
    value_path = context['ti'].xcom_pull(
        dag_id = 'VietNam_Transform',
        task_ids = 'Transform_VN',
        key = "file_path_covid_VN",
        include_prior_dates=True
        )
    df = pd.read_csv(value_path)
    df['last_updated'] = pd.to_datetime(df['last_updated']).dt.date

    print("value path:   ", value_path)


    print("Date file ne: ", df.info)
    date_file = df['last_updated'][0]
    #Read data from DB DES
    df_des = pd.read_sql("select last_updated from \"Covid_VietNam\" order by last_updated desc", engine_des); 
    df_des['last_updated'] = pd.to_datetime(df_des['last_updated']).dt.date

    date_des = df_des['last_updated'][0]


    if date_file == date_des:
        return "data_old"
    else:
        return "data_new"


# neu data cua ngay da co trong DB destinate
def _data_old(**context):
    engine_des = connection_des()

    ###Load into 
    value_path = context['ti'].xcom_pull(
        dag_id = 'VietNam_Transform',
        task_ids = 'Transform_VN',
        key = "file_path_covid_VN",
        include_prior_dates=True
        )
    df = pd.read_csv(value_path)
    print("Value path ne: ", value_path)
    df['last_updated'] = pd.to_datetime(df['last_updated']).dt.date
    date_file = df['last_updated'][0]
    print("Date file ne :.....: ", date_file)

    #Read data from DB DES
    df_des = pd.read_sql("select last_updated from \"Covid_VietNam\" order by last_updated desc", engine_des); 
    df_des['last_updated'] = pd.to_datetime(df_des['last_updated']).dt.date
    date_des = df_des['last_updated'][0]


    dbconnect = engine_des.raw_connection()
    
    query = "DELETE FROM \"Covid_VietNam\" WHERE last_updated = '" + str(date_des) + "'"
    cursor = dbconnect.cursor()
    cursor.execute(query)
    dbconnect.commit()
    df.to_sql('Covid_VietNam', engine_des, if_exists='append', index=False)
    print('Xoa xong roi insert')

# neu data cua ngay chua co trong DB destinate
def _data_new(**context):
    engine_des = connection_des()

    ###Load into 
    value_path = context['ti'].xcom_pull(
        dag_id = 'VietNam_Transform',
        task_ids = 'Transform_VN',
        key = "file_path_covid_VN",
        include_prior_dates=True
        )
    df = pd.read_csv(value_path)
    df['last_updated'] = pd.to_datetime(df['last_updated']).dt.date

    df.to_sql('Covid_VietNam', engine_des, if_exists='append', index=False)
    print('Insert nha')

def Notify():
    print("Notication: Flow ETL data covid Done")


#--------------------

   # case_vaccine_vn.to_sql('Covid_VietNam', engine_des, if_exists='append', index=False)

# [START how_to_task_group]
with DAG(dag_id="VN_Load_Covid",schedule_interval="@daily", start_date=datetime(2022, 4, 23),catchup=False,  tags=["ETL_VietNam"]) as dag:
    data_old = PythonOperator(
        task_id="data_old", python_callable=_data_old
    )
    data_new = PythonOperator(
        task_id="data_new", python_callable=_data_new
    )  
    check_condition = BranchPythonOperator(
        task_id="check_condition", python_callable=check_condition
    )
    notify = PythonOperator(
        task_id='notify_ETL_Done',
        python_callable = Notify,
        dag = dag,
        trigger_rule="none_failed"
    )
    check_condition >> [data_old, data_new] >> notify
    

 