# -*- coding: utf-8 -*-
from __future__ import print_function
import time
from builtins import range
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.oracle_operator import OracleOperator
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import urllib3
from sqlalchemy import types, create_engine,text
import os
import json
import requests
urllib3.disable_warnings()

"""
Тянем данные и.
"""

oracle_pass = BaseHook.get_connection("DWH_DEE").password
oracle_login = BaseHook.get_connection("DWH_DEE").login
oracle_host = BaseHook.get_connection("DWH_DEE").host

conn = create_engine(f'oracle+cx_oracle://{oracle_login}:{oracle_pass}@{oracle_host}/?service_name=DWH?encoding=utf8')


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
SERVICE_ACCOUNT_EMAIL = 'ukt-cloud@konic-progress-196909.iam.gserviceaccount.com'
DISCOVERY_URI = 'https://analyticsreporting.googleapis.com/$discovery/rest'
KEY_FILE_LOCATION = r'/home/airflow/keys/uktOwox-d00000000e12.p12'
VIEW_ID =  Variable.get('CorrectMasterDataID')



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 17),
    'email': ['mig@ukt.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
    }



dag = DAG(
    dag_id='daily_GaApp_toOracle', 
    default_args=default_args, 
    schedule_interval='30 6 * * *', description='Данные из GA и APP daily' )



def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_p12_keyfile(
        SERVICE_ACCOUNT_EMAIL, KEY_FILE_LOCATION, scopes=SCOPES)
    http = credentials.authorize(httplib2.Http())
    analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)
    return analytics

analytics = initialize_analyticsreporting()

token =  Variable.get('YandexAppMetricaToken')

def date_fromGA(date_st,date_sp):
    QUERY = {
        'reportRequests': [ # https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet?hl=ru#DimensionFilter
            {'viewId': VIEW_ID,
             'dateRanges': [{'startDate': date_st, 'endDate': date_sp}],
             "samplingLevel": 'LARGE',
             "dimensions": [{"name": "ga:date"}],
             'metrics': [{"expression": "ga:sessions"}],
             'pageSize': 10000}
        ]}

    request = analytics.reports().batchGet(body=QUERY).execute()  # запрос API

    API_data = request['reports'][0]['data']['rows']
    API_data = [list(i['dimensions']) + list(i['metrics'][0]['values']) for i in API_data]
    return API_data


def data_fromMetrika(date_st,date_sp):

    params_sess = {'lang': 'ru', 'request_domain': 'ru', 'id': 516239,
                   'date1': date_st, 'date2': date_sp, 'metrics': 'ym:s:sessions', 'dimensions': 'ym:s:date',
                   'sort': '-ym:s:date', 'offset': 1, 'limit': 61, 'accuracy': 1, 'proposedAccuracy': 'true'}
    response_ses = requests.get('https://api.appmetrica.yandex.ru/stat/v1/data', params=params_sess,
                                headers={'Authorization': 'OAuth '+token}).json()
    L = [[i['dimensions'][0]['name'], int(i['metrics'][0])] for i in response_ses['data']]
    L.sort()
    return L


def data_ToOracle(date_st,date_sp):
    

    GA = date_fromGA(date_st, date_sp)
    GA_df = pd.DataFrame.from_records(GA, columns=['date', 'session_ga'])
    GA_df['date'] = GA_df['date'].str[:4] + "-" + GA_df['date'].str[4:6] + "-" + GA_df['date'].str[-2:] # хрень
    GA_df['session_ga'] = GA_df['session_ga'].apply(np.int64)

    app = data_fromMetrika(date_st,date_sp)
    App_df = pd.DataFrame.from_records(app, columns=['date', 'session_app'])
    mergeData = pd.merge(GA_df, App_df)
    mergeData.date = pd.to_datetime(mergeData.date, format='%Y-%m-%d')    
    dtyp = {c: types.VARCHAR(mergeData[c].str.len().max()) for c in mergeData.columns[mergeData.dtypes == 'object'].tolist()}
    print(mergeData)
    mergeData.to_sql('ga_app_sess_day', conn,schema='dwh_stage', if_exists='append', index=False, dtype=dtyp)
    

day_ago='{{ macros.ds_add(ds, -7) }}'

task_1 = PythonOperator(
        task_id="data_fromGa", 
        python_callable=date_fromGA,
        op_args =[day_ago,'{{ yesterday_ds }}'],
        dag=dag)

task_2 = PythonOperator(
        task_id="data_fromMetrika", 
        python_callable=data_fromMetrika,
        op_args =[day_ago,'{{ yesterday_ds }}'],
        dag=dag)

task_3 =  OracleOperator(
    task_id='delete_last_7day',
    sql=f'''delete from dwh_stage.ga_app_sess_day where "date" >= to_date('{day_ago}', 'YYYY-MM-DD')''', 
    oracle_conn_id = 'DWH_DEE',autocommit=True,  dag=dag)

task_4 = PythonOperator(
        task_id="data_ToOracle", 
        python_callable=data_ToOracle,
        op_args =[day_ago,'{{ yesterday_ds }}'],
        dag=dag)

task_1>>task_3
task_2>>task_3
task_3>>task_4
