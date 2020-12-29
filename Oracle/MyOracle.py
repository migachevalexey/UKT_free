import datetime
import pprint
import time
import pandas as pd
import numpy as np
import cx_Oracle
import pandas_oracle.tools as pt
from sqlalchemy import types, create_engine
import os
from pandas.io import gbq, sql

"""Коннект к серверу ORACLE через различные lib'ы
"""

os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
currDate = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
pd.set_option('display.max_rows', 1000) # для вывода на  печать полного dataFrame
pd.set_option('display.max_columns', 80)
pd.set_option('display.width', 300)


con_sqlalchemy =create_engine('oracle+cx_oracle://DWH:DWH@srv-dwh1.ru/?service_name=DWH?encoding=utf8')
df_sqlalchemy = pd.read_sql_query('''select * from BANNER_CTR FETCH FIRST 5 ROWS ONLY ''', con_sqlalchemy)
print(df_sqlalchemy)


con_sqlalchemy =create_engine('oracle+cx_oracle://DWH:DWH@srv-dwh1.h.ru/?service_name=DWH?encoding=utf8')
df_sqlalchemy = con_sqlalchemy.execute('''delete from temp6 where "date" =  to_date('15-10-2019', 'DD-MM-YYYY') ''')
print(df_sqlalchemy)

currDate = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")

sql_action_id = f"""select distinct  action_id from temp5 where "date" != to_date('{currDate}', 'YYYYMMDD')"""
df_action_id = pd.read_sql_query(sql_action_id, con_sqlalchemy)
print(df_action_id['action_id'].values.tolist())


conn_Oracle = cx_Oracle.connect('DWH/DWH@srv-dwh1.ru/DWH')
cursor = conn_Oracle.cursor()
cursor.execute("select * from BANNER_CTR FETCH FIRST 5 ROWS ONLY")
pprint.pprint(cursor.fetchall())
cursor.close()
conn_Oracle.close()


conn_pandas_oracle = pt.open_connection("./pass/oracleconfig-test.yml")
query1 = "select count(*) qty from ACTION_PURCHASE"
query2 = "select * from APPSFLYER FETCH FIRST 5 ROWS ONLY"
df1 = pt.query_to_df(query1, conn_pandas_oracle, 10000)
df2 = pt.query_to_df(query2, conn_pandas_oracle, 10000)
print(df1)
print(df2)
pt.close_connection(conn_pandas_oracle)

with open(r'C:\Python\ukt\txtcsvFile\list_id_actiom.txt') as f:
    content = f.readlines()
# you may also want to remove whitespace characters like `\n` at the end of each line
content = [x.strip() for x in content]

conn_Oracle = cx_Oracle.connect('DWH/DWH@srv-dwh.ru/DWH')
cursor = conn_Oracle.cursor()
for i in content:

    sql=f"delete from ACTION_PURCHASE_ITEMS api where not exists(select * from ACTION_ITEM ai where ai.ACTION_ID=api.ACTION_ID and ai.MATERIAL_ID=api.MATERIAL_ID ) and api.ACTION_ID='{i}'"
    cursor.execute(sql)
    conn_Oracle.commit()
    # print(cursor.fetchall())
cursor.close()
conn_Oracle.close()

