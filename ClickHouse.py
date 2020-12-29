import datetime
import time
import pandas as pd
from clickhouse_driver import Client
import numpy as np
import requests
import pprint
import os

pd.set_option('display.max_rows', 1000) # для вывода на  печать полного dataFrame
pd.set_option('display.max_columns', 80)
pd.set_option('display.width', 300)

client = Client(host='rc1b-mn00000.mdb.yandexcloud.net', ca_certs='c:/Python/pass/YandexInternalRootCA.crt', secure=True,
                 user='user1', password='12345678', database='db1')
pprint.pprint(client.execute('SELECT * FROM system.processes'))


def request():
    url = 'https://{host}:8443/?database={db}&query={query}'.format(
        host='rc1b-123456.mdb.yandexcloud.net',
        db='db1',
        query='SELECT count(*) from db1.testopt')
    auth = {'X-ClickHouse-User': 'user1',
            'X-ClickHouse-Key': '12345678'}
    res = requests.get(url,headers=auth,verify='c:/Python/pass/YandexInternalRootCA.crt')
    res.raise_for_status()
    return res.text

# print(request())

df_cols=[f'colm{i}' for i in range(82)]
df_values = ['object', 'Int64', 'Int64', 'Int64', 'object', 'object', 'object', 'Int64', 'object', 'object', 'object',
             'object', 'object', 'object', 'object', 'object', 'Int64', 'object', 'Int64', 'object', 'object',
             'object', 'float64', 'float64', 'object', 'object', 'object', 'object', 'object', 'Int64', 'float64',
             'object', 'object', 'object', 'Int64', 'Int64', 'object', 'float64', 'float64', 'object', 'object', 'object', 'object',
             'Int64', 'object', 'object', 'float64', 'Int64', 'object', 'object', 'Int64', 'Int64', 'Int64', 'object',
             'object', 'object', 'object', 'object', 'object', 'object', 'object', 'object', 'object', 'Int64', 'Int64',
             'object', 'object', 'Int64', 'Int64', 'object', 'object', 'object', 'object', 'object', 'object',
             'float64', 'float64', 'object', 'object', 'object', 'object', 'Int64']

df_types = dict(zip(df_cols, df_values))
chunksize = 250000
CH_table = 'testlog'

def creataTable():
    with open('./txtcsvFile/TableSchemaFix.txt', encoding='utf-8') as f, \
            open('./txtcsvFile/TableSchema.txt', encoding='utf-8') as f1:
        TableSchemaFix = f.read()
        TableSchema = f1.read()
        # client.execute(f'CREATE TABLE IF NOT EXISTS {CH_table} {TableSchemaFix} ENGINE = MergeTree() PARTITION BY (colm4) ORDER BY (colm0, colm4) SETTINGS index_granularity=8192')
        client.execute( f'CREATE TABLE IF NOT EXISTS {CH_table} {TableSchema} ENGINE = Log')

# выбираем колонки где строка фиксирована и определяем ее длину
def fixstring():
    dflen = pd.DataFrame()
    df = pd.read_csv('C:/Python/ukt/txtcsvFile/1/2LIS_03_BF_201909.csv', nrows=1000, skiprows=1, names=df_cols,
                     dtype=df_types)
    df_object = df.select_dtypes(include=[object]).columns
    for i in df_object:
        try:
            dflen[f'{i}'] = df[f'{i}'].str.len().unique()
        except:
            continue
    pprint.pprint(dflen.to_dict(orient='records'))

def main (table):
    # for csv_file in os.listdir('./txtcsvFile/1'):
    print(datetime.datetime.now())
    for i, chunk in enumerate(pd.read_csv('C:/Python/ukt/txtcsvFile/1/2LIS_03_BF_201908.csv', chunksize=chunksize, skiprows=1, names=df_cols, dtype=df_types)):
            try:
                # print(chunk.dtypes)
                # chunk=chunk.replace(r'^\s*$', np.nan, regex=True)
                # определяем максимальные длинны строк, что бы в схеме делать FixedString(x)
                # measurer = np.vectorize(len)
                # df_object = chunk.select_dtypes(include=[object])
                # res2 = dict(zip(df_object, measurer(df_object.values.astype(str)).max(axis=0)))

                z = list(chunk.itertuples(index=False, name=None))
                client.execute(f'INSERT INTO {table}  VALUES', z, types_check=False)

            except: chunk.to_csv(f'./txtcsvFile/2/{csv_file[:-4]}_{i}.csv', index=False)
    print(datetime.datetime.now())
# creataTable()
main(CH_table)
