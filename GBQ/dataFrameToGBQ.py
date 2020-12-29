from google.cloud import bigquery
import MySQLdb
import json
import time
import datetime as dt
import pprint

import pandas as pd
from pandas.io import gbq

"""
   Тянем данные из базы ukt из таблицы ZAKAZ -  id заказа и статус.
   В BiqQuery делаем insert в таблицу My_Order_Params через  gbq.to_gbq
   и SQL-запросом обновляем status в таблице My_Orders 
"""

PROJECT_ID = 'konic-progress-196909'
projectid = '78997000000'  # http://prntscr.com/k0dus1
DATASET_ID = 'MatchedData'
TABLE = 'All_Orders'
import_action = 'append'  # append , 'replace' will overwrite the table if it exists, 'fail' will not overwrite the table if it exists.

client = bigquery.Client(project=PROJECT_ID)
dataset = client.dataset(DATASET_ID)


def dataToGBQ():
    df = pd.read_csv(r'C:\Python\ukt\txtcsvFile\appsflyers\organic-in-app-events_2019-01-01_2019-01-31.csv')
    z = len(df)
    df.columns = [i.replace(' ', '_') for i in list(df.columns.values)]
    # или так df.columns=df.columns.str.replace(' ','_')
    df = df.drop_duplicates()
    gbq.to_gbq(df, f'{DATASET_ID}.{TABLE}', projectid, if_exists=import_action)  # отправка данных в GBQ

    print(
        'Loaded {} row into {}. Начальный файл: {}строк. Было дубликатов - {} '.format(len(df), TABLE, z, z - len(df)))

sql="select distinct customer_user_id FROM `konic-progress-196909.Temp.iosnotOrders` " \
    "where  safe_cast(customer_user_id as int64) not in ( SELECT kpp FROM `konic-progress-196909.Mig_Data.Orders` where date>'2019-03-05')"

def dataFromGBQ():
    df = gbq.read_gbq(sql, projectid,dialect='standard')  # отправка данных в GBQ
    df.to_csv('ios.csv', index=False)
    # gbq.to_gbq(df, 'Mig_Data.AndryOrdersAppIos', projectid, if_exists=import_action)  # отправка данных в GBQ
    # z=len(df)
    # df = df.drop(['event_time_selected_timezone'], axis=1)
    # # df = df.drop_duplicates()
    # print(f'{len(df)}, {z}')
    # # Select duplicate rows except first occurrence based on all columns
    # duplicate = df[df.duplicated()]
    # duplicate.to_csv('duplicate.csv', sep=';',index =False)


def main():
    # dataToGBQ()
    dataFromGBQ()


if __name__ == '__main__':
    main()
