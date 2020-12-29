# -*- coding: utf-8 -*-
from __future__ import print_function
import time
from builtins import range
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.oracle_operator import OracleOperator
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
Тянем данные из GA device и groupChannel на еженедельной основе.
"""

oracle_pass = BaseHook.get_connection("DWH_DEE").password
oracle_login = BaseHook.get_connection("DWH_DEE").login
oracle_host = BaseHook.get_connection("DWH_DEE").host

conn = create_engine(f'oracle+cx_oracle://{oracle_login}:{oracle_pass}@{oracle_host}/?service_name=DWH?encoding=utf8')

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
SERVICE_ACCOUNT_EMAIL = 'ukt-cloud@konic-progress-196909.iam.gserviceaccount.com'
DISCOVERY_URI = 'https://analyticsreporting.googleapis.com/$discovery/rest'
#KEY_FILE_LOCATION = r'/usr/local/airflow/dags/keys/uktOwox-d00000000e12.p12'
KEY_FILE_LOCATION = r'/home/airflow/keys/uktOwox-d00000000e12.p12'
VIEW_ID =  Variable.get('CorrectMasterDataID')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 15),
    'email': ['mig@ukt.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
    }

dag = DAG(
    dag_id='week_GA_toOracle', 
    default_args=default_args, 
    schedule_interval='30 6 * * 1', description='Данные из GA device и groupChannel week' )


def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_p12_keyfile(
        SERVICE_ACCOUNT_EMAIL, KEY_FILE_LOCATION, scopes=SCOPES)
    http = credentials.authorize(httplib2.Http())
    analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)
    return analytics

analytics = initialize_analyticsreporting()


def weekFromGA(ga_dim, date_start, date_stop):
    QUERY = {
        'reportRequests': [
            {'viewId': VIEW_ID,
             'dateRanges': [{'startDate': date_start, 'endDate': date_stop}],
             "samplingLevel": 'LARGE',
             "dimensions": [{"name": "ga:isoYearIsoWeek"}, {"name": ga_dim}],
             'metrics': [{"expression": "ga:sessions"}, {"expression": "ga:transactions"},
                         {"expression": "ga:transactionRevenue"}, ],
             'pageSize': 10000}
        ]}
    request = analytics.reports().batchGet(body=QUERY).execute()  # запрос API
    rowCount = request['reports'][0]['data']['rowCount']
    API_data = request['reports'][0]['data']['rows']

    print(f'Колво записей: {rowCount}')
    API_data = [list(i['dimensions']) + list(i['metrics'][0]['values']) for i in API_data]

    return API_data


def weekToOracle(date_start, date_stop):
    data_ga_device = weekFromGA("ga:deviceCategory", date_start, date_stop)
    data_ga_gr = weekFromGA("ga:channelGrouping", date_start, date_stop)

    for i in [[data_ga_device, 'device', 'ga_device_sess_week'], [data_ga_gr, 'channel_group', 'ga_channel_sess_week']]:
        i[0] = pd.DataFrame(i[0], columns=['year_week', i[1], 'sessions', 'transactions', 'revenue'])
        i[0]['revenue'] = i[0]['revenue'].astype(float).round(0)
        i[0][['sessions', 'transactions','revenue']] = i[0][['sessions', 'transactions','revenue']].astype(int)
        dtyp = {c: types.VARCHAR(i[0][c].str.len().max()) for c in i[0].columns[i[0].dtypes == 'object'].tolist()}
        i[0].to_sql(i[2], conn, schema='dwh_stage',if_exists='append', index=False, dtype=dtyp)

    
task = PythonOperator(
        task_id="weekGA_toOracle", 
        python_callable=weekToOracle,
        op_args =['{{ macros.ds_add(ds, -7) }}', '{{ yesterday_ds }}'],
        # provide_context=True,
        dag=dag)
