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


from datetime import datetime, timedelta, date
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

apps = {'com.akz.apps.ukt': 'iOS', 'ru.ukt.android.uktid': 'Android'}
token = Variable.get('YandexAppMetricaToken')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 15),
    'email': ['mig@ukt.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=60)
    }


dag = DAG(
    dag_id='week_App_ToOracle', 
    default_args=default_args, 
    schedule_interval='30 6 * * 1', 
    description='Данные из APP weekly' )


def week_FromAppMetrika(d_start, d_stop):
    wNum= int((datetime.strptime(d_start, '%Y-%m-%d').date()).strftime("%Y%V"))
    d = []
    for i,j in apps.items():
        params_sess = {'lang': 'ru', 'request_domain': 'ru',
                       'filters': "exists ym:d:device with (appID=='{}')".format(i), 'id': 516000,
                       'date1': d_start, 'date2': d_stop, 'metrics': 'ym:s:sessions', 'dimensions': 'ym:s:date',
                       'sort': '-ym:s:date', 'offset': 1, 'limit': 10, 'accuracy': 1, 'proposedAccuracy': 'true'}
        response_ses = requests.get('https://api.appmetrica.yandex.ru/stat/v1/data', params=params_sess,
                                    headers={'Authorization': 'OAuth ' + token}).json()
        params_users = {'lang': 'ru', 'request_domain': 'ru',
                        'filters': "exists ym:d:device with (appID=='{}')".format(i), 'id': 516000,
                        'date1': d_start, 'date2': d_stop, 'metrics': 'ym:u:activeUsers',
                        'dimensions': 'ym:u:date', 'sort': '-ym:u:date', 'include_undefined': 'true', 'offset': 1,
                        'limit': 10, 'accuracy': 1, 'proposedAccuracy': 'true'}
        response_user = requests.get('https://api.appmetrica.yandex.ru/stat/v1/data', params=params_users,
            headers={'Authorization': 'OAuth ' + token}).json()

        d.append([wNum, j,  int(*response_ses['totals']),int(*response_user['totals'])])
    # assert len(d)>1

    df_app_weekly=pd.DataFrame(d, columns=['year_week','app','sessions','users'])

    dtyp = {c: types.VARCHAR(df_app_weekly[c].str.len().max()) for c in df_app_weekly.columns[df_app_weekly.dtypes == 'object'].tolist()}
    df_app_weekly.to_sql('app_sess_user_week', conn, schema='dwh_stage', if_exists='append', index=False, dtype=dtyp)


insert_week1 = PythonOperator(
        task_id="week1_App_ToOracle", 
        python_callable=week_FromAppMetrika,
        op_args =['{{ macros.ds_add(ds, -7) }}','{{ yesterday_ds }}'],
        dag=dag)

delete_week2 =  OracleOperator(
    task_id='delete_last_week',
    sql=f'''delete FROM dwh_stage.app_sess_user_week where YEAR_WEEK in (SELECT MAX(YEAR_WEEK) FROM dwh_stage.app_sess_user_week)''', 
    oracle_conn_id = 'DWH_DEE', autocommit=True,  dag=dag)

insert_week2 = PythonOperator(
        task_id="week2_App_ToOracle", 
        python_callable=week_FromAppMetrika,
        op_args =['{{ macros.ds_add(ds, -14) }}','{{ macros.ds_add(ds, -8) }}'],
        dag=dag)


delete_week2>>insert_week1>>insert_week2
