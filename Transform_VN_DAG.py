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



def Transform_VN_DAG(**kwargs):
    engine_des = connection_des()
    engine_stage = connection_stage()

    #Dataframe case, vaccine from connect stage
    df_case_vn = pd.read_sql("select * from \"vietnam_case\"", engine_stage); 
    df_vaccine_vn = pd.read_sql("select * from \"vietnam_vaccine\"", engine_stage); 

    #Dataframe Pronvice from Des
    df_province = pd.read_sql("select * from \"Province\"",engine_des)

    #Join df_case_vn với df_vaccine_vn
    join_temp = pd.merge(df_case_vn, df_vaccine_vn, how='inner', left_on = ('name','last_updated'), right_on = ('name','last_updated'), suffixes=('_left','_right'))
    case_vaccine_VN =  join_temp[['name','new_case','new_death','new_recovered','new_active','total_case','total_death','death_average_7days','case_average_7days','percent_case_population','lastOnceInjected','lastTwiceInjected','popOverEighteen','totalInjected','totalOnceInjected','totalTwiceInjected','totalVaccineProvided','totalVaccineAllocatedReality','totalVaccineAllocated','currentTeamInjectInPractice','last_updated']] 
    df_case_vaccine_VN = pd.DataFrame(case_vaccine_VN)
    df_case_vaccine_VN.insert(0, 'ID', range(1, 1 + len(df_case_vaccine_VN))) #thêm cột khóa chính, mã tự tăng

    #Join df_case_vaccine_VN vs df_Province

    join_temp_1 = df_case_vaccine_VN.set_index('name').join(df_province.set_index('ProvinceName'))

    case_vaccine_vn =  join_temp_1[['ID','ProvinceID','new_case','new_death','new_recovered','new_active','total_case','total_death','death_average_7days','case_average_7days','percent_case_population','lastOnceInjected','lastTwiceInjected','popOverEighteen','totalInjected','totalOnceInjected','totalTwiceInjected','totalVaccineProvided','totalVaccineAllocatedReality','totalVaccineAllocated','currentTeamInjectInPractice','last_updated']] 

    ####################################################
    ###Transform Datatype:

    case_vaccine_vn['ProvinceID'] = case_vaccine_vn['ProvinceID'].fillna(0)
    case_vaccine_vn['ProvinceID']  = case_vaccine_vn['ProvinceID'] .astype('int64')

    case_vaccine_vn['last_updated'] = pd.to_datetime(case_vaccine_vn['last_updated']).dt.date

    ###Load into 
    case_vaccine_vn.to_sql('Covid_VietNam', engine_des, if_exists='append', index=False)

    #String Ngày run của Dag Run
    date_run = kwargs['ds']

    print('Date run ne', date_run)

    file = 'vn_covid_case_vaccine' + date_run + '.csv'
    
    case_vaccine_vn.to_csv(file,index=False, encoding="utf-8")

    kwargs['ti'].xcom_push(key="file_path_covid_VN", value = file)




    
# [START how_to_task_group]
with DAG(dag_id="VietNam_Transform",schedule_interval="@daily", start_date=datetime(2022, 5, 28),catchup=False,  tags=["ETL_VietNam"]) as dag:

    Transform_VN = PythonOperator(
        task_id ='Transform_VN',
        python_callable=Transform_VN_DAG,
        dag = dag,
        provide_context = True
    ) 

    Transform_VN 

