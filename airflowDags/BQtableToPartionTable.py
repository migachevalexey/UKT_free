# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 17),
    'email': ['mig@ukt.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=7)
}

dag = DAG(
    dag_id='BQ_TableToPartTable', 
    default_args=default_args, 
    schedule_interval='35 6 * * *', description='Перезаписывает и добавляет данные в PARTITIONTIME таблице'
    , catchup=False
)

t2 = BigQueryCheckOperator(task_id='data_validation',
                           sql="""SELECT count(*) FROM `konic-progress-196909.Mig_Data.Orders` where date = '{{ ds }}'""", use_legacy_sql=False, dag=dag)


t3 = BigQueryOperator(
    task_id='delete_22_day_ago',
    use_legacy_sql=False,
    allow_large_results=True,
    sql="""delete FROM Mig_Data.PartOrders where  DATE(_PARTITIONTIME) >= '{{ macros.ds_add(ds, -22) }}' and DATE(_PARTITIONTIME) <= '{{ ds }}'""",  
    dag=dag)

t2>>t3


for i in range(0,23,1):
	k='{{{{ (execution_date - macros.timedelta(days={})).strftime("%Y-%m-%d") }}}}'.format(i)
	k_='{{{{ (execution_date - macros.timedelta(days={})).strftime("%Y%m%d") }}}}'.format(i)
	t4 = BigQueryOperator(
		task_id='insert_into_PartTable_day_{}'.format(i),
		write_disposition='WRITE_APPEND',
		use_legacy_sql=False,
		allow_large_results=True,
		sql="""SELECT * FROM `konic-progress-196909.Mig_Data.Orders` where date = '{}'""".format(k),
        destination_dataset_table='konic-progress-196909.Mig_Data.PartOrders${}'.format(k_),
		dag=dag)
	t3 >> t4
