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
import json
import csv, urllib.request

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


@task()
def Extract_Data_Covid_World(**context):

    #String Ngày run của Dag Run
    date_run = context["execution_date"].strftime("%m-%d-%Y")

    #Lấy URL ghép với Ngày RUN 
    url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/" + date_run + ".csv"

    df = pd.read_csv(url)
    engine = connection_stage()
    df.to_sql('World_Covid_Case', engine, if_exists='append', index=False)
    print('----------')
    print(date_run)
    print('----------')


# [START how_to_task_group]
with DAG(dag_id="Test_Demo_BackFilling",schedule_interval='@daily', start_date=datetime(2022, 5, 15),catchup=True,  tags=["ETL"]) as dag:

    with TaskGroup("Extract_Load_Stage", tooltip="Extract data and load to stage") as  Extract_Load_Stage:
        Extract_Data_Covid_World = Extract_Data_Covid_World()
        #define task order
        [Extract_Data_Covid_World]

    Extract_Load_Stage

 