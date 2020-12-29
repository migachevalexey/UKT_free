import datetime
from google.cloud import bigquery
import MySQLdb
import json
from pandas.io import gbq
import pandas as pd


PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'Temp'

file_db_connect = 'C:/Python/ukt/pass/MySQL_db_connect.json'
client = bigquery.Client(project=PROJECT_ID)
dataset = client.dataset(DATASET_ID)

DateStart = (datetime.date.today() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
DateStop = (datetime.date.today() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")


def DfFromSQL():
    with open(file_db_connect) as f:
        param_сonnect = json.load(f)
    db_connect = MySQLdb.connect(user=param_сonnect['user'], passwd=param_сonnect['passwd'],
                                 host=param_сonnect['host'], db=param_сonnect['db_sess'], charset='cp1251')

    sql_query = "SELECT master_order_id,flag_cancel,pay_sum FROM ZAKAZ " \
                "where Date(created) between '{}' and '{}' and archive=0".format(DateStart, DateStop)
    df_mysql = pd.read_sql(sql_query, con=db_connect)
    db_connect.close()
    gbq.to_gbq(df_mysql, f'Mig_Data.Lud', '78997000000', if_exists='replace')  # отправка данных в GBQ


DfFromSQL()
