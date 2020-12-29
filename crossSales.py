import json
from google.cloud import bigquery
import itertools
import pandas as pd
from pandas.io import gbq
import MySQLdb


""" Формирует таблицу с кросс продажами по категориям(заданы явно).
Основной товар и что  с ним покупалось. Начальные данные беруться из MySQL DB"""

pd.set_option('display.max_rows', 1000) # для печати полного dataFrame
pd.set_option('display.max_columns', 12)
pd.set_option('display.width', 300)

file_db_connect = 'C:/Python/ukt/pass/MySQL_db_connect.json'

def sql_db_select():
    with open(file_db_connect) as f:
        param_сonnect = json.load(f)
    db_connect = MySQLdb.connect(user=param_сonnect['user'], passwd=param_сonnect['passwd'],
                                 host=param_сonnect['host'],  charset='cp1251')

    sql_data = "select distinct z.ZAKAZ_ID,ifnull(purchase_group_id,-1) as group_id from `ukt_sess`.`ZAK` z " \
                 "left join `ukt_sess`.`ZAK_ITEMS` zi on (zi.zakaz_id=z.zakaz_id) " \
                 "left join `ukt_prod`.`ukt_purch_group_goods` gg on (gg.goods_id=zi.item_id) " \
                 "where DATE(z.DATA) between '2019-01-01' and '2019-04-30' and z.date_ans is not NULL and z.ANS is not NULL and z.STATUS>0 and z.archive<>1"

    df_mysql = pd.read_sql(sql_data, con=db_connect)
    gbq.to_gbq(df_mysql, f'Temp.QQ', '78997000000', if_exists='append')  # отправка данных в GBQ

# sql_db_select()

def crossSale():
    projectid = '78997000000'

    Cat=[28,34]
    crossCat = list(range(1,43))+[46,48,53,55,59,61,63,66]
    comb_Cats = list(itertools.product(crossCat,Cat))

    df_empty = pd.DataFrame(index=crossCat, columns=list(map(str,Cat)))
    df_empty.fillna(0, inplace=True)
    # df_empty1 = df_empty.copy()

    for i in comb_Cats:
        sql = "with a as (select *, count( ZAKAZ_ID ) OVER (PARTITION BY ZAKAZ_ID ORDER BY ZAKAZ_ID ) as row " \
              "from Temp.LudaQQ where ZAKAZ_ID in (select * from `Temp.LudaOrders`) and group_id in ({}) ) " \
              "select count(distinct ZAKAZ_ID) as qnt from a where row>1".format(','.join(list(map(str,i))))

        df_sql = gbq.read_gbq(sql, projectid, dialect='standard')

        if len(df_sql)>0:
            df_empty[str(i[1])][i[0]] = df_sql.iloc[0]['qnt']
            # df_empty1[str(i[1])][i[0]] = df_sql.iloc[1]['qnt']
        else: continue

    print(df_empty)
    df_empty.to_excel('dfd11.xlsx', sheet_name='sv')

crossSale()