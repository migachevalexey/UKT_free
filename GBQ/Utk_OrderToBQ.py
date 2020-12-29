from google.cloud import bigquery
import MySQLdb
import json
from pandas.io import gbq
import pandas as pd


'''
Аналог файла из Cron, но с возможностью менять даты.
Нужен - если что то не загрузилось автоматом по cron 
Важное замечание - table_schema - указываем полю нужный тип данных и первый раз  запускаем to_gbq с параметром  table_schema=table_schema
При этом сама таблица создается в GBQ автоматически
При последующих загрузках table_schema=table_schema - указывать не надо
'''


PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'ukt'
TABLE = 'My_Orders'
file_db_connect = 'C:/Python/ukt/pass/MySQL_db_connect.json'
client = bigquery.Client(project=PROJECT_ID)
dataset = client.dataset(DATASET_ID)

projectid = '78997000000'
import_action = 'append'  # 'replace' will overwrite the table if it exists, 'fail' will not overwrite the table if it exists.

SQL_date_start = '2018-10-07'
SQL_date_stop = '2018-10-09'


def sql_db_select():
    with open(file_db_connect) as f:
        param_сonnect = json.load(f)
    db_connect = MySQLdb.connect(user=param_сonnect['user'], passwd=param_сonnect['passwd'],
                                 host=param_сonnect['host'], db=param_сonnect['db_sess'], charset='cp1251'
                                 )
    cursor = db_connect.cursor()
    sql_query = "SELECT distinct z.master_order_id,cast(created as date) as date, os.title as status, sg.title as order_source, is_mobile_browser as mobile_browser " \
                "FROM ukt_sess.ZAK z, zakaz_data zd, ukt_order_status os, ukt_sale_group sg, ukt_order_placing op where cast(created as date) between '{}' and '{}' and z.ZAKAZ_ID=zd.ZAKAZ_ID " \
                "and os.id=order_status_id and op.pav_order_id=z.master_order_id and sg.sap_id=op.sale_group and sg.title!='Android old app'".format(SQL_date_start, SQL_date_stop)
    cursor.execute(sql_query)
    sql_data = cursor.fetchall()  # r=cursor.fetchmany(5)
    cursor.close()
    db_connect.close()

    return sql_data


def stream_dataToBQ():
    ins_data = list(sql_db_select())
    labels = ['pav_order_id', 'date', 'status', 'order_source', 'mobile_browser']
    df = pd.DataFrame.from_records(ins_data, columns=labels)
    table_schema = [{'name': 'pav_order_id', 'type': 'string'},
                    {'name': 'date', 'type': 'string'},
                    {'name': 'status', 'type': 'STRING'},
                    {'name': 'order_source', 'type': 'STRING'},
                    {'name': 'mobile_browser', 'type': 'INT64'}]
    gbq.to_gbq(df, f'{DATASET_ID}.{TABLE}', projectid, if_exists=import_action)  # отправка данных в GBQ

def main():
    stream_dataToBQ()

if __name__ == '__main__':
    main()