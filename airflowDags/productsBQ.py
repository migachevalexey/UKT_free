# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, date

from google.cloud import bigquery
import MySQLdb
import json
import pandas as pd
from pandas.io import gbq
import os

'''
   Тянем вновь созданные товары и акции из базы ukt из таблицы ITEM и ukt_promoaction за неделю
   item_id, original_id, name, category, brand - отправляем в BQ, т.к. в стримнге OWOX только артикулы 
   id, name, start, stop - по АКЦИЯМ
'''

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/airflow/keys/konic-progress-196909-086050000000.json"
PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'Mig_Data'
client = bigquery.Client(project=PROJECT_ID)
runFileDir = os.path.dirname(__file__)
#pathPass = os.path.join(runFileDir, os.path.normpath(r'../keys/connections_key.json'))
#pathLog = os.path.join(runFileDir, os.path.normpath(r'../logs/Item_Action_toBQ.txt'))
pathPass = '/home/airflow/keys/connections_key.json'
pathLog = '/home/airflow/logs/Item_Action_toBQ.txt'


day7 = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")


def sql_db_select():  # надо переделать на Pandas pd.read_sql
    with open(pathPass) as f:
        param_сonnect = json.load(f)
    db_connect = MySQLdb.connect(user=param_сonnect['user'], passwd=param_сonnect['passwd'],
                                 host=param_сonnect['host'], db=param_сonnect['db_prod'], charset='cp1251'
                                 )
    cursor = db_connect.cursor()
    sqlQueryItem = f"SELECT i.item_id, i.original_id, replace(replace(i.name,',' , '.'), '\n',''),replace(c.name,',','.') as category, replace(pv.value,',','.') as brand " \
                "from ITEM i " \
                "left join CATALOGUE c on c.id = i.catalogue_id " \
                "left join ukt_property_mar pm on pm.item_id = i.item_id and pm.property_id = 479 " \
                "left join ukt_property_value pv on pv.id = pm.property_value_id " \
                f"where Date(new_date) >'{day7}'"

    cursor.execute(sqlQueryItem)
    data_item = cursor.fetchall()  # r=cursor.fetchmany(5)

    sqlQueryAction = f"select promoaction_id, name, Date(start) as start, date(finish) as stop from ukt_promoaction  where  Date(start)>'{day7}'"

    cursor.execute(sqlQueryAction)
    date_action = cursor.fetchall()

    db_connect.close()
    return list(data_item),list(date_action)

def queryData(QUERY, tab):
    query_job = client.query(QUERY)
    rows = query_job.result()
    if 'MERGE'.upper() in QUERY:
        print(query_job.state, query_job.num_dml_affected_rows, "rows affected")
        with open(pathLog, 'a') as f:
            f.write(f'on date: {date.today()} ' + ' load ' + str(query_job.num_dml_affected_rows) + f' rows in table {tab} '+ query_job.state+'\n')
    return rows

def stream_dataToBQ():
    item, action = sql_db_select()
    schema_item = [{'name': 'item_id', 'type': 'string'},
                    {'name': 'original_id', 'type': 'string'},
                    {'name': 'name', 'type': 'STRING'},
                    {'name': 'category', 'type': 'STRING'},
                    {'name': 'brand', 'type': 'STRING'}]

    schema_action = [{'name': 'id', 'type': 'INTEGER'},
                   {'name': 'name', 'type': 'STRING'},
                   {'name': 'start', 'type': 'STRING'},
                   {'name': 'stop', 'type': 'STRING'}]

    dfItem = pd.DataFrame.from_records(item,  columns=['item_id', 'original_id', 'name', 'category', 'brand'])
    dfAction = pd.DataFrame.from_records(action, columns=['id', 'name', 'start', 'stop'])
    gbq.to_gbq(dfItem, f'{DATASET_ID}.temp_item', '78997000000', if_exists='replace', table_schema=schema_item)
    gbq.to_gbq(dfAction, f'{DATASET_ID}.temp_action', '78997000000', if_exists='replace', table_schema=schema_action)

    insertItem = f'MERGE {DATASET_ID}.uktItems D ' \
                   f'USING {DATASET_ID}.temp_item S ' \
                   f'ON D.item_id = S.item_id ' \
                   f'WHEN NOT MATCHED THEN ' \
                   f'INSERT (item_id, original_id, name, category, brand) ' \
                   f'VALUES(item_id, original_id, name, category, brand)'

    insertAction = f'MERGE {DATASET_ID}.uktAction D ' \
                 f'USING {DATASET_ID}.temp_action S ' \
                 f'ON D.id = S.id ' \
                 f'WHEN NOT MATCHED THEN ' \
                 f'INSERT (id, name, start, stop) ' \
                 f'VALUES(id, name, start, stop)'

    queryData(insertItem, 'uktItems')
    queryData(insertAction, 'uktAction')

    table_ref = client.dataset(DATASET_ID).table('temp_item')
    client.delete_table(table_ref)
    table_ref = client.dataset(DATASET_ID).table('temp_action')
    client.delete_table(table_ref)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 14),
    'email': ['mig@ukt.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=60)}

dag = DAG(
    dag_id='Actions_ItemsToBQ',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    description='Name, brand, category товара в GBQ')

task_1 = PythonOperator(
    task_id='task_Actions_ItemsToBQ', python_callable=stream_dataToBQ, dag=dag)

