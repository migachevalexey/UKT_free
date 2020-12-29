# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import types, create_engine
from pandas.io import gbq
from google.cloud import bigquery
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta, date
from airflow.hooks.base_hook import BaseHook

"""
   Забираем данные из стриминга appsflyer. Android и iOS - это две разные таблицы с разной структурой в GBQ
   Структура таблиц различается не сильно
   Объединяем данные в одну таблицу и отправляем ее в Oracle
"""

os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/airflow/keys/konic-progress-196909-086050000000.json"
PROJECT_ID = 'konic-progress-196909'
projectid = '78997000000'
client = bigquery.Client(project=PROJECT_ID)

oracle_pass = BaseHook.get_connection("DWH_DEE").password
oracle_login = BaseHook.get_connection("DWH_DEE").login
oracle_host = BaseHook.get_connection("DWH_DEE").host
conn = create_engine(
    f'oracle+cx_oracle://{oracle_login}:{oracle_pass}@{oracle_host}/?service_name=DWH?encoding=utf8')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 11, 15),
    'email': ['mig@ukt.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
}


dag = DAG(
    dag_id='AppsFlyer_toOracle_Stage',
    default_args=default_args, 
    schedule_interval='0 8 * * *',
    description='Данные AppsFlyer из GBQ в Oracle'
)


def dataFromBQ(yesterday_ds):
    sql_and = fr"select * , DATE(_PARTITIONTIME) as date FROM `AppsFlyer.android_events` WHERE DATE(_PARTITIONTIME) = '{yesterday_ds}'"
    sql_ios = f"select *, DATE(_PARTITIONTIME) as date FROM `AppsFlyer.ios_events`  WHERE DATE(_PARTITIONTIME) = '{yesterday_ds}'"

    df_and = gbq.read_gbq(sql_and, projectid, dialect='standard')
    df_ios = gbq.read_gbq(sql_ios, projectid, dialect='standard')

    df_and = df_and.reindex(columns=df_and.columns.tolist() + ['device_name'])
    df_ios = df_ios.reindex(
        columns=df_ios.columns.tolist() + ['operator', 'android_id', 'device_brand', 'validated', 'advertising_id',
                                           'carrier', 'device_model', 'IMEI'])
    df_ios = df_ios[df_and.columns.tolist()]

    FinalDF = pd.concat([df_and, df_ios], sort=False)

    FinalDF[['click_time', 'attributed_touch_time', 'download_time', 'install_time', 'event_time',
             'download_time_selected_timezone', 'click_time_selected_timezone', 'install_time_selected_timezone',
             'event_time_selected_timezone', 'date']] = \
        FinalDF[['click_time', 'attributed_touch_time', 'download_time', 'install_time', 'event_time',
                 'download_time_selected_timezone', 'click_time_selected_timezone', 'install_time_selected_timezone',
                 'event_time_selected_timezone', 'date']].apply(pd.to_datetime)
    # убираем символы " и af_ - они дают сильный прирост в длине поле event_value
    FinalDF['event_value'] = FinalDF['event_value'].str.replace('"|af_', '', regex=True)

    return FinalDF


def dataToOracle(n,yesterday_ds):
    dataBQ = dataFromBQ(yesterday_ds)

    dtyp = {c: types.VARCHAR(dataBQ[c].astype(str).str.len().max())
            for c in dataBQ.columns[dataBQ.dtypes == 'object'].tolist()}

    # если len(event_value)>4000(бывает при больших заказах), тогда берем только первые 4000
    dataBQ['event_value'] = dataBQ['event_value'].map(
        lambda x: x[:4000] if (x is not None and len(x) > 4000) else x)

    for i in range(0, len(dataBQ), n):
        try:
            dataBQ.iloc[i:i + n, :].to_sql('appsflyer', conn, schema='dwh_stage',if_exists='append', index=False, dtype=dtyp)
        except:
            z = dataBQ.iloc[i:i + n, :]
            m = 5
            for j in range(0, len(z), m):
                try:
                    z.iloc[j:j+m, :].to_sql('appsflyer', conn, schema='dwh_stage', if_exists='append', index=False, dtype=dtyp)
                except:
                    z.iloc[j:j+m, :].to_csv(f'/home/airflow/logs/Amplitude_toOracle/exception_{yesterday_ds}_{i}.csv', index=False)
                    continue

    return str(len(dataBQ))

task_1 = PythonOperator(
    task_id='appsflyer_toOracle',  python_callable=dataToOracle, op_args =[50000,'{{ yesterday_ds }}'], dag=dag)

email_task = EmailOperator(
    to='mig@ukt.ru',
    task_id='email_task',
    subject="Load {{ task_instance.xcom_pull(task_ids='appsflyer_toOracle') }} records to appsflyer za {{ yesterday_ds }} is True",
    html_content=" Insert {{ task_instance.xcom_pull(task_ids='appsflyer_toOracle') }} records from konic-progress-196909.AppsFlyer to dwh_stage.appsflyer",
    dag=dag)

task_1>>email_task
