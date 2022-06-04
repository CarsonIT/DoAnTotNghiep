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

def load_to_destination(**kwargs):
    engine_des = connection_des()

    ###Load into 
    value_path = kwargs['ti'].xcom_pull(
        dag_id = 'VietNam_Transform',
        task_ids = 'Transform_VN',
        key = "file_path_covid_VN",
        include_prior_dates=True
        )
    df = pd.read_csv(value_path)
    df['last_updated'] = pd.to_datetime(df['last_updated']).dt.date
    print('DF_CSVVVVVVVVVV',df['last_updated'][0])
    date_file = df['last_updated'][0]
    #Read data from DB DES
    df_des = pd.read_sql("select last_updated from \"Covid_VietNam\" order by last_updated desc", engine_des); 
    df_des['last_updated'] = pd.to_datetime(df_des['last_updated']).dt.date
    print('DF_DESSSSSSSSSS',df_des['last_updated'][0])
    date_des = df_des['last_updated'][0]
    print(df_des.info())

    query = "DELETE FROM \"Covid_VietNam\" WHERE last_updated = '" + str(date_des) + "'"
    if date_file == date_des:
        dbconnect = engine_des.raw_connection()
        cursor = dbconnect.cursor()
        cursor.execute(query)
        dbconnect.commit()
        df.to_sql('Covid_VietNam', engine_des, if_exists='append', index=False)
        print('Xoa xong roi insert')
    else:
        df.to_sql('Covid_VietNam', engine_des, if_exists='append', index=False)
        print('Insert nha')




   # case_vaccine_vn.to_sql('Covid_VietNam', engine_des, if_exists='append', index=False)

# [START how_to_task_group]
with DAG(dag_id="Demo_LOAD",schedule_interval="25 21 * * *", start_date=datetime(2022, 4, 23),catchup=False,  tags=["ETL_VietNam"]) as dag:

    LoadCovid_VN = PythonOperator(
    task_id ='LoadCovid_VN',
    python_callable=load_to_destination,
    dag = dag,
    provide_context = True
    ) 

    LoadCovid_VN



 