import datetime
from google.cloud import bigquery
import MySQLdb
import json
from pandas.io import gbq
import pandas as pd


"""
Перенес это действие в файл OrderToBQ. В Cron больше не учавствует!

Тянем оплаченные деньги и статус отмены за последние 3 недели из siteDB и обновляем эти поля в таблице Mig_Data.Orders
Через временную таблицу, ее вконце удаляем. Результат пишем в лог-файл

"""


PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'Mig_Data'
TABLE_Orders = 'Orders'
TABLE_Params = 'OrdersStatus'
file_db_connect = 'C:/Python/ukt/pass/MySQL_db_connect.json'
client = bigquery.Client(project=PROJECT_ID)
dataset = client.dataset(DATASET_ID)

DateStart = (datetime.date.today() - datetime.timedelta(days=22)).strftime("%Y-%m-%d")
DateStop = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


def sql_to_GBQ():
    with open(file_db_connect) as f:
        param_сonnect = json.load(f)
    db_connect = MySQLdb.connect(user=param_сonnect['user'], passwd=param_сonnect['passwd'],
                                 host=param_сonnect['host'], db=param_сonnect['db_sess'], charset='cp1251')

    sql_query = "SELECT master_order_id as transaction,flag_cancel as cancel,pay_sum as money FROM ZAK " \
                "where Date(created) between '{}' and '{}' and archive=0".format(DateStart, DateStop)

    df_mysql = pd.read_sql(sql_query, con=db_connect)
    gbq.to_gbq(df_mysql, f'{DATASET_ID}.{TABLE_Params}', '78997000000', if_exists='replace')  # отправка данных в GBQ


def queryData(QUERY):
    query_job = client.query(QUERY)
    rows = query_job.result()
    if 'UPDATE' in QUERY.upper():
        print(query_job.state, query_job.num_dml_affected_rows, "rows affected")
        with open(r'c:\python\ukt\Cron\log\OrdersFullToBQ.txt', 'a') as f:
            f.write(f'На дату: {DateStop} ' + f' обновлен cancel_status,money у {str(query_job.num_dml_affected_rows)} записей в таблице Mig_Data.Orders' + '\n')
    return rows


def main():
    sql_to_GBQ()
    QUERY_upd = f"UPDATE `{DATASET_ID}.{TABLE_Orders}` T  SET T.canceled = S.cancel, T.money = S.money FROM `{DATASET_ID}.{TABLE_Params}` S " \
                f"WHERE T.transaction = S.transaction and (T.canceled != S.cancel or T.money != S.money)"
    queryData(QUERY_upd)
    table_ref = client.dataset(DATASET_ID).table(TABLE_Params)
    if client.get_table(table_ref):
        client.delete_table(table_ref)


if __name__ == '__main__':
    main()