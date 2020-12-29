from google.cloud import bigquery
import MySQLdb
import json
import datetime
import pandas as pd


PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'Temp'
Part_TABLE = '123P'
file_db_connect = 'C:/Python/ukt/pass/MySQL_db_connect.json'
client = bigquery.Client(project=PROJECT_ID)
dataset = client.dataset(DATASET_ID)

startDate = (datetime.date.today() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
stopDate = (datetime.date.today() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
#dates = pd.date_range('20130101', periods=5) - полезно на будущее


""" УСТАРЕЛО!! 
_PARTITIONTIME TABLE
Тянем данные из MySQL и отправляем их в GBQ через чистый Pandas методом load_table_from_dataframe
Важное замечание - изначально данные отправляем в обычную таблицу(ee формирует сам метод load_table_from_dataframe), 
чтобы ее сформировать с нужным ТИПОМ полей
избежать этой хрени   ins_data = [[int(i[0]), i[1], i[2], str(i[3])] for i in sql_db_select()]
Потом руками делаем _PARTITIONTIME TABLE и автоматом грузим туда данные
Обратить внимание на эту чушь с index=pd.Index([start... - формирует индекс  равный текущей дате
"""

def sql_db_select():
    with open(file_db_connect) as f:
        param_сonnect = json.load(f)
    db_connect = MySQLdb.connect(user=param_сonnect['user'], passwd=param_сonnect['passwd'],
                                 host=param_сonnect['host'], db=param_сonnect['db_sess'], charset='cp1251'
                                 )
    cursor = db_connect.cursor()
    sql_query = "SELECT  z.master_order_id, os.title as status, sg.title as order_source, is_mobile_browser as mobile_browser " \
                "FROM ukt_sess.ZAK z, zakaz_data zd, ukt_order_status os, ukt_sale_group sg, ukt_order_placing op where cast(created as date) between '{}' and '{}' and z.ZAKAZ_ID=zd.ZAKAZ_ID " \
                "and os.id=order_status_id and op.pav_order_id=z.master_order_id and sg.sap_id=op.sale_group and sg.title!='Android old'".format(startDate, stopDate)
    cursor.execute(sql_query)
    sql_data = cursor.fetchall()  # r=cursor.fetchmany(5)
    cursor.close()
    db_connect.close()
    return sql_data

'''
РАБОЧИЙ ВАРИАНТ!!!
'''
def stream_dataToBQ():
    table_ref = dataset.table(Part_TABLE + f'${startDate.strftime("%Y%m%d")}')
    ins_data = [[int(i[0]), i[1], i[2], str(i[3])] for i in sql_db_select()]

    labels = ['pav_order_id', 'status', 'order_source', 'mobile_browser']
    df = pd.DataFrame.from_records(ins_data,
                                   index=pd.Index([startDate.strftime("%Y-%m-%d")]*len(ins_data), name='date'),
                                   columns=labels)
    job = client.load_table_from_dataframe(df, table_ref)
    job.result()
    assert job.state == 'DONE'
    print('Loaded {} row into Table {}'.format(len(ins_data), table_ref.table_id))

'''
РАБОЧИЙ ВАРИАНТ streaming insert!!!
'''
# def stream_dataToBQ():
#     ins_data = sql_db_select()
#     table_ref = dataset.table(Part_TABLE + f'${startDate.strftime("%Y%m%d")}')
#     table = client.get_table(table_ref)
#     for i in range(0, len(ins_data) + 1, 10000):
#         errors = client.insert_rows(table, ins_data[i:i + 10000])  # API request
#         assert errors == []
#     if not errors:
#         print('Loaded {} row into {}'.format(len(ins_data), Part_TABLE+ f'${startDate.strftime("%Y%m%d")}'))
#     else:
#         print('Errors:')
#         pprint.pprint(errors)


def main():
    stream_dataToBQ()


if __name__ == '__main__':
    main()