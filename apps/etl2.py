from zipfile import ZipFile
import requests
import json
import pandas as pd
import os
import datetime
import sqlalchemy
from sqlalchemy import create_engine, MetaData, select, Table
import time
from sqlalchemy.types import Integer,DateTime,String,Numeric



#engine = sqlalchemy.create_engine('postgresql://cnwcerliarfbvk:0091d0f3d6abbc8f5545f122703dfb43ee0da1b18e2d59282a96cc62373060af@ec2-63-32-248-14.eu-west-1.compute.amazonaws.com:5432/d886nbel7m1jvm')
engine = sqlalchemy.create_engine('postgresql://mpxwygfqyhlykh:5ba21ce8429a2bfd66cb38edcaa4d28a276f28843f666edb54aa4f4e9e55f67a@ec2-54-194-211-183.eu-west-1.compute.amazonaws.com:5432/d8l6qs2difee9t')
#engine = sqlalchemy.create_engine('postgresql://postgres:123456@localhost:5432/geogps')
metadata = MetaData()
connection = engine.connect()


car_table = Table('cars_test', metadata, autoload=True,autoload_with=engine)
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
df_zone = pd.DataFrame()

#Find necessary dates
today = datetime.date.today()




stmt_last_date = 'SELECT distinct time_out FROM zone order by time_out desc limit 1'
results = connection.execute(stmt_last_date).scalar()
print(results)
#last_date_synced=datetime.datetime.strptime("01.01.2022 00:00:00", '%d.%m.%Y %H:%M:%S')
last_date_synced=results.date()
print(last_date_synced)

def gps_api():
  global df_reis, df_eco, df_odo,df_zone
  first_date = last_date_synced
  second_date = today

  zero_date = datetime.datetime(1970, 1, 1)
  zero_date=zero_date.date()
  difference_first_date = first_date-zero_date
  difference_second_date = second_date-zero_date
  total_seconds_first_date = difference_first_date.total_seconds()
  total_seconds_second_date = difference_second_date.total_seconds()
  total_seconds_first_date = int(total_seconds_first_date)
  #total_seconds_first_date =1654027200
  total_seconds_second_date = int(total_seconds_second_date)
  #total_seconds_second_date = 1661976000

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
    url_part2 = "https://local.geogps.ge/wialon/ajax.html?svc=report/exec_report&params={\"reportResourceId\":349,\"reportTemplateId\":27,\"reportObjectId\":"

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
      print(row[0])
      zones_raw = pd.read_excel('alo_xlsx.xlsx', sheet_name=row[0], skipfooter=1,
                              dtype={'index': int, 'plate': str, 'reg_zone': str, 'time_in': datetime, 'time_out': datetime,
                                     'duration_in': datetime, 'off_time': datetime, 'driver': str})

      zones_raw['plate']=row[0]
      zones_raw = zones_raw.drop(zones_raw.columns[[0]], axis=1)
      df_zone = df_zone.append(zones_raw, ignore_index=True)
    except:
      pass

  sc_reg.columns = ['plate', 'service_center','region']
  zone_ag_data = df_zone.merge(sc_reg, on='plate', how='left')
  zone_ag_data.columns=['reg_zone','time_in','time_out','duration_in_min','off_time_min','driver','plate','service_center','region']


  zone_ag_data['off_time_min'] = pd.to_timedelta(zone_ag_data['off_time_min'].astype(str)).dt.total_seconds()//60
  zone_ag_data['duration_in_min'] = pd.to_timedelta(zone_ag_data['duration_in_min'].astype(str)).dt.total_seconds()//60
  #zone_ag_data['off_time']= zone_ag_data['off_time'].astype(float)
  #zone_ag_data['duration_in']= zone_ag_data['duration_in'].astype(float)
  #zone_ag_data['off_time']=zone_ag_data['off_time'].round(decimals=2)
  #zone_ag_data['duration_in']=zone_ag_data['duration_in'].round(decimals=2)
  zone_ag_data.to_sql('zone', connection, if_exists='append',
                      dtype={'index': Integer(), 'reg_zone': String(), 'time_in': DateTime(),
                             'time_out': DateTime(),
                             'duration_in_min': Integer(), 'off_time_min': Integer(),'driver': String(), 'plate': String()
                             ,'service_center': String(),'region': String()})

  print("ETL finished")

if last_date_synced==(today - datetime.timedelta(days=1)):
  print("Database up to date")
else:
  gps_api()













