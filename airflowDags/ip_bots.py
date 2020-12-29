# -*- coding: utf-8 -*-
import os
import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta, date

sys.path.append('/home/airflow/dags/function')
#sys.path.append('/function')
#runFileDir=os.path.dirname(__file__)
#print('Путь',runFileDir)

from ip_filtering import  fix_bots

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 12, 6),
    'email': ['mig@ukt.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=60)
}


dag = DAG(
    dag_id='ip_bots',
    default_args=default_args, 
    schedule_interval='40 8 * * *'
)


task_1 = PythonOperator(
    task_id='ip_bots',  python_callable=fix_bots, op_args=['{{ ds_nodash }}'],  dag=dag)

email_task = EmailOperator(
    to='mig@ukt.ru',
    task_id='email_task',
    subject=" ip bots {{ yesterday_ds }} ",
    html_content="{{task_instance.xcom_pull(task_ids='ip_bots') }}",
    dag=dag)

task_1>>email_task

