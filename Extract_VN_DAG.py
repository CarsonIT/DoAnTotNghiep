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
import pendulum
from airflow.exceptions import AirflowFailException
from airflow.sensors.python import PythonSensor
from airflow.models import Variable


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

def Load_to_Stage():
    # rut trich va load vao Stage data ca nhiem VN
    url = "https://covid19.ncsc.gov.vn/api/v3/covid/provinces"
    df = pd.read_json(url)    
    engine = connection_stage()
    df.to_sql('vietnam_case', engine,  if_exists='replace', index=False)

    # rut trich va load vao Stage data vaccine VN
    url = "https://covid19.ncsc.gov.vn/api/v3/vaccine/provinces"
    df = pd.read_json(url) 
    engine = connection_stage()
    df.to_sql('vietnam_vaccine', engine, if_exists='replace', index=False)


def Extract_Data_Case_VN():
    # rut trich va load vao Stage data ca nhiem VN
    try:
        url = "https://covid19.ncsc.gov.vn/api/v3/covid/provinces"
        df = pd.read_json(url)  

        date_temp = Variable.get('temp_date')
        print("Data temp ***** : ", date_temp)
        if (date_temp != df['last_updated'][0]):
            date_temp = df['last_updated'][0]
            Variable.update(key='temp_date', value=date_temp)
            return True
        return False
        
    except Exception as e:
        raise ValueError('Email to DataEngineer about error that: ',e)
        return False


def Extract_Data_Vaccine_VN():
    try:
        url = "https://covid19.ncsc.gov.vn/api/v3/vaccine/provinces"
        df = pd.read_json(url)  

        date_temp_1 = Variable.get('temp_date_1')
        print("Data temp ***** : ", date_temp_1)
        if (date_temp_1 != df['last_updated'][0]):
            date_temp_1 = df['last_updated'][0]
            Variable.update(key='temp_date_1', value=date_temp_1)
            return True
        return False

    except Exception as e:
        raise ValueError('Email to DataEngineer about error that: ',e)
        return False



# [START how_to_task_group]
with DAG(dag_id="Extract_CaseAndVac_VN_1",schedule_interval="@hourly", start_date=datetime(2022, 6, 4),catchup=False,  tags=["ETL_VietNam"]) as dag:

    wait_for_data_case_new = PythonSensor(
        task_id="wait_for_data_case_new",
        python_callable=Extract_Data_Case_VN,
        poke_interval = 10*60,  # Poke every 10 minutes
        timeout = 3550,
        mode='reschedule',  # Timeout of 1 hours
        dag=dag,
    )

    wait_for_data_vac_new = PythonSensor(
        task_id="wait_for_data_vac_new",
        python_callable=Extract_Data_Vaccine_VN,
        poke_interval = 60,  # Poke every 10 minutes
        timeout = 30, 
        mode='reschedule', # Timeout of 1 hours
        dag=dag,
    )

    Load_to_Stage = PythonOperator(
        task_id ='Load_to_Stage',
        python_callable=Load_to_Stage,
        dag = dag,
    ) 

    [wait_for_data_case_new, wait_for_data_vac_new] >> Load_to_Stage
 