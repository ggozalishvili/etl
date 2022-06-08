from zipfile import ZipFile
import requests
import json
import pandas as pd
import os
import datetime
import sqlalchemy
from sqlalchemy import create_engine, MetaData, select, Table
import time
from sqlalchemy.types import Integer,DateTime,String



#engine = sqlalchemy.create_engine('postgresql://cnwcerliarfbvk:0091d0f3d6abbc8f5545f122703dfb43ee0da1b18e2d59282a96cc62373060af@ec2-63-32-248-14.eu-west-1.compute.amazonaws.com:5432/d886nbel7m1jvm')
engine = sqlalchemy.create_engine('postgresql://postgres:123456@localhost:5432/geogps')
metadata = MetaData()
connection = engine.connect()


car_table = Table('cars', metadata, autoload=True,autoload_with=engine)
sc_mapping = Table('sc_mapping', metadata, autoload=True,autoload_with=engine)

stmt = select([car_table])
results = connection.execute(stmt).fetchall()

stmt_sc = select([sc_mapping])
results_sc_map = connection.execute(stmt_sc).fetchall()

#Final Dataframes for SQL
car = pd.DataFrame(results)
sc_reg = pd.DataFrame(results_sc_map)
df_odo = pd.DataFrame()
df_reis = pd.DataFrame()
df_eco = pd.DataFrame()


#Find necessary dates
today = datetime.date.today()




#stmt_last_date = 'SELECT distinct start_time FROM reis order by start_time desc limit 1'
#results = connection.execute(stmt_last_date).scalar()
#print(results)
last_date_synced=datetime.datetime.strptime("01.01.2022 00:00:00", '%d.%m.%Y %H:%M:%S')
last_date_synced=last_date_synced.date()


def gps_api():
  global df_reis, df_eco, df_odo
  first_date = last_date_synced
  second_date = today

  zero_date = datetime.datetime(1970, 1, 1)
  zero_date=zero_date.date()
  difference_first_date = first_date-zero_date
  difference_second_date = second_date-zero_date
  total_seconds_first_date = difference_first_date.total_seconds()
  total_seconds_second_date = difference_second_date.total_seconds()
  total_seconds_first_date = int(total_seconds_first_date)
  total_seconds_second_date = int(total_seconds_second_date)

  #login
  url = "https://local.geogps.ge/wialon/ajax.html?svc=token/login&params={\"token\": \"1a40157dde2cd7675367fe3a97caa6938AA5FB511914F8CF5F9A60DCC1D6AE2A95ED5EC6\",\"operateAs\": \"\"}"

  payload={}
  headers = {
    'Cookie': 'sessions=b3e3c71a05fe1060898f43f585f600c4'
  }

  response = requests.request("POST", url, headers=headers, data=payload)

  d = json.loads(response.text)
  key = d['eid']

  #select
  url_part = "https://local.geogps.ge/wialon/ajax.html?svc=core/search_items&params={\"spec\":{\"itemsType\":\"avl_unit\",\"propName\":\"sys_name\",\"propValueMask\":\"*"
  url_part1_1 ="*\",\"sortType\":\"sys_name\"},\"force\":1,\"flags\":1025,\"from\":0,\"to\":0}&sid="

  payload1 = {}
  files = {}
  headers = {
    'Cookie': 'sessions=30a81ecaefb6a3679018236478993143'
  }

  for index, row in car.iterrows():
    url1 = url_part+row.item()+url_part1_1+key
    connected = False
    response1 = []
    placeholder = 1

    try:
      response1 = requests.request("POST", url1, headers=headers, data=payload1, files=files)

    except Exception as e:
      print(str(datetime.time(datetime.now()))[:8] + str(e))  # To log the errors

      time.sleep(0.5)

    e = json.loads(response1.text)
    key1 = e['items']
    a_key = "id"
    values_of_key = [a_dict[a_key] for a_dict in key1]
    values_of_key=str(values_of_key)
    values_of_key = values_of_key.replace('[', '')
    values_of_key = values_of_key.replace(']', '')

    #execute report
    url_part2 = "https://local.geogps.ge/wialon/ajax.html?svc=report/exec_report&params={\"reportResourceId\":349,\"reportTemplateId\":15,\"reportObjectId\":"

    url_part2_2_a = ",\"reportObjectSecId\":0,\"interval\":{\"from\":"
    url_part2_2_b = ",\"to\":"
    url_part2_2_c = ",\"flags\":0}}&sid="
    url_part2_2 = url_part2_2_a + str(total_seconds_first_date) + url_part2_2_b + str(total_seconds_second_date) + url_part2_2_c


    url2 = url_part2+values_of_key+url_part2_2+key
    payload2={}
    headers = {
      'Cookie': 'sessions=30a81ecaefb6a3679018236478993143'
    }

    response2 = requests.request("POST", url2, headers=headers, data=payload2)

    #export report
    url_part3 = "https://local.geogps.ge/wialon/ajax.html?svc=report/export_result&params={\"format\":8, \"outputFileName\":\"alo.xlsx\"}&sid="
    url3 = url_part3+key

    payload3={}
    headers = {
      'Cookie': 'sessions=30a81ecaefb6a3679018236478993143'
    }

    response3 = requests.request("POST", url3, headers=headers, data=payload3)
    try:
      with open("../data.zip", 'wb') as f:
        f.write(response3.content)

      with ZipFile("../data.zip", 'r') as zipObj:
        # Extract all the contents of zip file in current directory
        zipObj.extractall()

      odo_raw = pd.read_excel('alo_xlsx.xlsx',sheet_name=1,skipfooter=1,
                              dtype={'index': int, 'plate': str, 'start_odo': int, 'last_odo': int, 'odo_milage': int,
                                     'odo_travel_time': str, 'odo_max_speed': int, 'calc_odo': int})
      reis_raw = pd.read_excel('alo_xlsx.xlsx',sheet_name=2,skipfooter=1,
                               dtype={'index': int, 'plate': str, 'start_time': datetime,'start_location': str,
                                      'end_time': datetime, 'end_location': str, 'duration': str, 'milage': int,
                                      'max_speed': int, 'driver': str, 'fuel_consumed': int, 'quantity': int})
      eco_raw = pd.read_excel('alo_xlsx.xlsx',sheet_name=3,skipfooter=1,
                              dtype={'index': int, 'plate': str,'driver': str,'start_time': datetime, 'location': str,
                                      'penalty_type': str, 'penalty_points': int, 'quantity': int,
                                     'eco_milage': int})



      df_reis = df_reis.append(reis_raw,ignore_index = True)

      df_eco = df_eco.append(eco_raw,ignore_index = True)
      df_odo = df_odo.append(odo_raw[:1], ignore_index=True)

    except:
      pass

  df_reis = df_reis.drop(df_reis.columns[[11]], axis=1)
  df_reis.columns = ['plate', 'start_time', 'start_location', 'end_time', 'end_location','duration', 'milage', 'max_speed',
                             'driver','fuel_consumed','quantity']

  df_eco = df_eco.drop(df_eco.columns[[8, 9]], axis=1)
  df_eco.columns = ['plate', 'driver', 'start_time','location','penalty_type','penalty_points','quantity','eco_milage']
  df_odo.columns = ['plate', 'start_odo', 'last_odo','odo_milage','odo_travel_time','odo_max_speed']
  df_odo['calc_odo'] = df_odo['last_odo']+df_odo['odo_milage']
  sc_reg.columns = ['plate', 'service_center','region']

  df_reis['end_time'] = pd.to_datetime(df_reis['end_time'], format='%d.%m.%Y %H:%M:%S')
  df_reis['start_time'] = pd.to_datetime(df_reis['start_time'], format='%d.%m.%Y %H:%M:%S')
  df_eco['start_time'] = pd.to_datetime(df_eco['start_time'], format='%d.%m.%Y %H:%M:%S')


  reisebi_ag_data = df_reis.merge(sc_reg, on='plate', how='left')
  eco_ag_data = df_eco.merge(sc_reg, on='plate', how='left')
  odo_ag_data = df_odo.merge(sc_reg, on='plate', how='left')


  reisebi_ag_data.to_sql('reis', connection, if_exists = 'append',
                 dtype={'index': Integer(), 'plate': String(), 'start_time': DateTime(), 'start_location': String(),
                        'end_time': DateTime(), 'end_location': String(), 'duration': String(), 'milage': Integer(),
                        'max_speed': Integer(), 'driver': String(), 'fuel_consumed': Integer(), 'quantity': Integer()
                        , 'service_center': String(), 'region': String()})
  eco_ag_data.to_sql('eco', connection, if_exists = 'append',
                dtype={'index': Integer(), 'plate': String(), 'driver': String(), 'start_time': DateTime(), 'location': String(),
                       'penalty_type': String(), 'penalty_points': Integer(), 'quantity': Integer(),
                       'eco_milage': Integer(), 'service_center': String(), 'region': String()})
  odo_ag_data.to_sql('odo', connection, if_exists = 'replace',
                dtype={'index': Integer(), 'plate': String(), 'start_odo':  Integer(), 'last_odo': Integer(), 'odo_milage': Integer(),
                       'odo_travel_time': String(), 'odo_max_speed': Integer(), 'calc_odo': Integer()
                       ,'service_center': String(), 'region': String()})


  print("ETL finished")


if last_date_synced==(today - datetime.timedelta(days=1)):
  print("Database up to date")
else:
  gps_api()













