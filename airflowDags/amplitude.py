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

'''
Тянем данные из потока amplitude BQ и отправляем их в Oracle на ежедневной основе.
2 таблицы: events и events_properties. Таблицу events_properties отправляем пачками, т.к. очень большая
Загрузка данных в таблицы выполняется параллельно. 
Помним, что данные в amplitude BQ тоже отправляются скриптом(вытягиваем их из сервиса Amplitude) и появляются они там в 10:30 по мск 
Если загрузка завершилась ошибкой - отправляем сообщение на email
'''

os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/airflow/keys/konic-progress-196909-086050000000.json"
PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'ukt'
projectid = '78997000000'
client = bigquery.Client(project=PROJECT_ID)

oracle_pass = BaseHook.get_connection("DWH_DEE").password
oracle_login = BaseHook.get_connection("DWH_DEE").login
oracle_host = BaseHook.get_connection("DWH_DEE").host
conn = create_engine(f'oracle+cx_oracle://{oracle_login}:{oracle_pass}@{oracle_host}/?service_name=DWH?encoding=utf8')


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 12, 4),
    'email': ['mig@ukt.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}


dag = DAG(
    dag_id='Amplitude_toOracle_Stage',
    default_args=default_args, 
    schedule_interval='40 8 * * *',
    description='Данные Amplitude из GBQ в Oracle'
)

def sendEvents(yesterday_ds, **kwargs):

    sql_events = fr"select DATE(_PARTITIONTIME) as date, * FROM `amplitude.events` WHERE DATE(_PARTITIONTIME) = '{yesterday_ds}'"
    df_events = gbq.read_gbq(sql_events, projectid, dialect='standard')
    df_events.date = pd.to_datetime(df_events.date, format='%Y-%m-%d')
    dtyp = {c: types.VARCHAR(df_events[c].str.len().max())
                   for c in df_events.columns[df_events.dtypes == 'object'].tolist()}
    df_events.to_sql('amplitude_events', conn, schema='dwh_stage', if_exists='append', index=False, dtype=dtyp)
    #conn.execute("""ALTER INDEX AMPLITUDE_EVENTS_DATE_INDEX VISIBLE""")

    return  str(len(df_events))


def sendEventsProp(n,yesterday_ds):

    sql_events_prop = f"select *,DATE(_PARTITIONTIME) as date FROM `amplitude.events_properties` WHERE DATE(_PARTITIONTIME) = '{yesterday_ds}'"
    df_events_prop = gbq.read_gbq(sql_events_prop, projectid, dialect='standard')
    dtyp = {c: types.VARCHAR(df_events_prop[c].str.len().max())
               for c in df_events_prop.columns[df_events_prop.dtypes == 'object'].tolist()}

    for i in range(0, len(df_events_prop), n):
        df_events_prop.iloc[i:i + n, :].to_sql('amplitude_events_properties', conn, schema='dwh_stage', if_exists='append', index=False,
                                               dtype=dtyp)
    return  str(len(df_events_prop))


task_events = PythonOperator(
    task_id='events_toOracle', python_callable=sendEvents,  provide_context=True,  dag=dag)

task_events_prop = PythonOperator(
    task_id='events_prop_toOracle', python_callable=sendEventsProp, op_args =[150000, '{{ yesterday_ds }}'],  dag=dag)


email_task_event = EmailOperator(
    to='mig@ukt.ru',
    task_id='email_task_event',
    subject="Load {{ task_instance.xcom_pull(task_ids='events_toOracle') }} records to amplitude_events za {{ yesterday_ds }} is True",
    html_content=" Insert {{ task_instance.xcom_pull(task_ids='events_toOracle') }} records from konic-progress-196909.amplitude to dwh_stage.amplitude_events",
    dag=dag)

email_task_prop = EmailOperator(
    to='mig@ukt.ru',
    task_id='email_task_prop',
    subject="Load {{ task_instance.xcom_pull(task_ids='events_prop_toOracle') }} records to ampitude_properties {{ yesterday_ds }} is True",
    html_content=" Insert {{ task_instance.xcom_pull(task_ids='events_prop_toOracle') }} records from konic-progress-196909.amplitude to dwh_stage.amplitude_events_properties",
    dag=dag)


task_events >> email_task_event
task_events_prop >> email_task_prop
