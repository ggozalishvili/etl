from zipfile import ZipFile
import requests
import json
import pandas as pd
import os
import datetime
import sqlalchemy
from sqlalchemy import create_engine, MetaData, select, Table
import time
engine = sqlalchemy.create_engine('postgresql://oswrsssbmcdtsa:ad6cca6bc8a6d58a80313746f2f7ad22b12ae65d7f75b447f7b785b27845d9e8@ec2-176-34-211-0.eu-west-1.compute.amazonaws.com:5432/d6j6etm7h53s01')
metadata = MetaData()
connection = engine.connect()


car_table = Table('cars', metadata, autoload=True,
autoload_with=engine)
stmt = select([car_table])
results = connection.execute(stmt).fetchall()


#Final Dataframes for SQL
car = pd.DataFrame(results)
df_odo = pd.DataFrame()
df_reis = pd.DataFrame()
df_eco = pd.DataFrame()


#Find necessary dates
today = datetime.date.today()




#stmt_last_date = 'SELECT distinct start_time FROM reis order by start_time desc limit 1'
#results = connection.execute(stmt_last_date).scalar()
#print(results)
last_date_synced=datetime.datetime.strptime("01.01.2021 00:00:00", '%d.%m.%Y %H:%M:%S')
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

      odo_raw = pd.read_excel('alo_xlsx.xlsx',sheet_name=1,skipfooter=1)
      reis_raw = pd.read_excel('alo_xlsx.xlsx',sheet_name=2,skipfooter=1)
      eco_raw = pd.read_excel('alo_xlsx.xlsx',sheet_name=3,skipfooter=1)



      df_reis = df_reis.append(reis_raw,ignore_index = True)
      df_eco = df_eco.append(eco_raw,ignore_index = True)
      df_odo = df_odo.append(odo_raw[:1], ignore_index=True)

    except:
      pass




  df_reis.columns = ['plate', 'start_time', 'start_location', 'end_time', 'end_location','duration', 'milage', 'max_speed',
                             'driver','fuel_consumed','quantity']


  df_eco.columns = ['plate', 'driver', 'start_time','location','penalty_type','penalty_points','quantity','eco_milage']

  df_odo.columns = ['plate', 'start_odo', 'last_odo','odo_milage','odo_travel_time','odo_max_speed']
  df_odo['calc_odo']=df_odo['last_odo']+df_odo['odo_milage']


  df_reis.to_sql('reis', engine, if_exists = 'append')
  df_eco.to_sql('eco', engine, if_exists = 'append')
  df_odo.to_sql('odo', engine, if_exists = 'replace')


  print("ETL finished")


if last_date_synced==(today - datetime.timedelta(days=1)):
  print("Database up to date")
else:
  gps_api()













