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
import numpy as np
import psycopg2



#Here we will create python functions which are tasks these functions can call by a python operator. The below code is that we request the JSON data from the URL and write JSON data into CSV file format.
# The below function will connect to Postgres data by specifying the credentials and creating the driver_data table in the specified database, then will read the stored CSV file row by row and insert it into the table.

####### Extract
@task()
def Load_Continent():
    # load continent vao DB DestinationCovid
    # Trong dữ liệu data thế giới, dòng nào continent = null thì dòng đó là dữ liệu của 1 châu lục (chứ không phải 1 quốc gia)
    conn_stage = BaseHook.get_connection('StageCovid')
    engine_stage = create_engine(f'postgresql://{conn_stage.login}:{conn_stage.password}@{conn_stage.host}:{conn_stage.port}/{conn_stage.schema}')
    #dbcon_stage = engine.connect()
    df = pd.read_sql("select * from \"world_covid\"", engine_stage); 
    conn_des = BaseHook.get_connection('DestinationCovid')
    engine_des = create_engine(f'postgresql://{conn_des.login}:{conn_des.password}@{conn_des.host}:{conn_des.port}/{conn_des.schema}')

    # Các giá trị của Continent
    continents =  df[['continent','location','population']] #lấy các cột cần
    df_continents = pd.DataFrame(continents) # đọc data
    df_continents = df_continents[df_continents['continent'].isnull()] #show ra các giá trị null
    df_continents = df_continents[df_continents["location"].str.contains("High income|International|Low income|Lower middle income|Upper middle income|World") == False] # trong cột location, bỏ những giá trị không phải là châu lục
    #Xử lý kiểu dữ liệu của population thành INT
    df_continents['population'] = pd.to_numeric(df_continents['population'])   
    df_continents['population'] = df_continents['population'].fillna(0) # những giá trị NA thì thay bằng 0
    df_continents['population']  = df_continents['population'] .astype('int64') 
    df_temp = df_continents[['location','population']]
    #today = pd.to_datetime("today")
    df_temp.insert(0, 'New_ID', range(1, 1 + len(df_temp))) #thêm cột khóa chính, mã tự tăng
    df_temp.columns =['ContinentID','ContinentName','Population']
    df_temp.insert(3, 'Last_updated', pd.Timestamp.today().strftime('%Y-%m-%d')) #thêm cột last_update là ngày hiện tại

    #Convert Last_updated to Date
    df_temp['Last_updated'] = pd.to_datetime(df_temp['Last_updated']).dt.date

    df_temp.to_sql('Continent', engine_des, if_exists='replace', index=False)

@task()
def Load_Country():
    # load country vao DB DestinationCovid
    conn_stage = BaseHook.get_connection('StageCovid')
    engine_stage = create_engine(f'postgresql://{conn_stage.login}:{conn_stage.password}@{conn_stage.host}:{conn_stage.port}/{conn_stage.schema}')
    #dbcon_stage = engine.connect()
    df = pd.read_sql("select * from \"world_covid\"", engine_stage); # nếu lỗi chuyển thành db_con_stage
    conn_des = BaseHook.get_connection('DestinationCovid')
    engine_des = create_engine(f'postgresql://{conn_des.login}:{conn_des.password}@{conn_des.host}:{conn_des.port}/{conn_des.schema}')

    # load country vao DB DestinationCovid
    locations =  df[['location','population']]
    df_locations = pd.DataFrame(locations)
    df_locations.columns =['location','population']
    #Xử lý kiểu dữ liệu của population thành INT
    df_locations['population'] = pd.to_numeric(df_locations['population'])
    df_locations['population'] = df_locations['population'].fillna(0)
    df_locations['population']  = df_locations['population'] .astype('int64') 
    df_locations.insert(0, 'New_ID', range(1, 1 + len(df_locations))) #thêm cột khóa chính, mã tự tăng
    df_locations.columns =['CountryID','CountryName','Population']
    df_locations.insert(3, 'Last_updated', pd.Timestamp.today().strftime('%Y-%m-%d')) #thêm cột last_update là ngày hiện tại

    #Convert Last_updated to Date
    df_locations['Last_updated'] = pd.to_datetime(df_locations['Last_updated']).dt.date

    df_locations.to_sql('Country', engine_des, if_exists='replace', index=False)  

@task()
def Load_Province_Region():
    # load province va Region VN vao DB DestinationCovid
    url = "https://raw.githubusercontent.com/phucjeya/TTKD-10_DATH/main/Tinh_KhuVuc_Vn.csv"
    df = pd.read_csv(url)
    conn = BaseHook.get_connection('DestinationCovid')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    locations =  df[['Số thứ tự','Tên tỉnh, thành phố','Khu vực','Dân số người']]
    df_locations = pd.DataFrame(locations)
    df_locations.columns =['ProvinceID','ProvinceName','RegionName','Population']

    # create a list of our conditions
    conditions = [
        (df_locations['RegionName'] == "Đồng bằng sông Cửu Long"),
        (df_locations['RegionName'] == "Đông Nam Bộ"),
        (df_locations['RegionName'] == "Đông Bắc Bộ"),
        (df_locations['RegionName'] == "Đồng bằng sông Hồng"),
        (df_locations['RegionName'] == "Tây Nguyên"),
        (df_locations['RegionName'] == "Duyên hải Nam Trung Bộ"),
        (df_locations['RegionName'] == "Tây Bắc Bộ"),
        (df_locations['RegionName'] == "Bắc Trung Bộ")
        ]
    # create a list of the values we want to assign for each condition
    values = [1,2,3,4,5,6,7,8]
    # create a new column and use np.select to assign values to it using our lists as arguments
    df_locations['RegionID'] = np.select(conditions, values)
    #display updated DataFrame
    # lấy những cột cần để vào bảng province
    temp = df_locations[['ProvinceID','ProvinceName','Population','RegionID']]
    temp.insert(4, 'Last_updated', pd.Timestamp.today().strftime('%Y-%m-%d')) #thêm cột last_update là ngày hiện tại
    df_provinces = pd.DataFrame(temp)

    #Convert Last_updated to Date
    df_provinces['Last_updated'] = pd.to_datetime(df_provinces['Last_updated']).dt.date

    df_provinces['ProvinceName'].mask(df_provinces['ProvinceName'] == 'Thành phố Hồ Chí Minh', 'TP HCM', inplace=True) # transform dữ liệu: nếu giá trị là Thành phố Hồ Chí Minh --> TP HCM
    # lấy những cột cần để vào bảng region
    temp2 = df_locations[['RegionID','RegionName']]
    temp2.insert(2, 'Last_updated', pd.Timestamp.today().strftime('%Y-%m-%d')) #thêm cột last_update là ngày hiện tại
    df_region = pd.DataFrame(temp2)

    #Convert Last_updated to Date
    df_region['Last_updated'] = pd.to_datetime(df_region['Last_updated']).dt.date
    df_region = df_region.drop_duplicates()
    #load vào DB
    df_provinces.to_sql('Province', engine, if_exists='replace', index=False)
    df_region.to_sql('Region', engine, if_exists='replace', index=False)



# [START how_to_task_group]
with DAG(dag_id="ETL_SubTables",schedule_interval="0 0 1 * *", start_date=datetime(2022, 4, 23),catchup=False,  tags=["product_model"]) as dag: # lập lịch chạy 1 tháng 1 lần

    with TaskGroup("ETL_SubTable", tooltip="Load data to SubTable in DB DestinationCovid") as  ETL_SubTable:
        Load_Continent = Load_Continent()
        Load_Country = Load_Country()
        Load_Province_Region = Load_Province_Region()
        [Load_Continent,Load_Country,Load_Province_Region]
    ETL_SubTable 

 