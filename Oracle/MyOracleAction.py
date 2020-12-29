import datetime
import json
import pprint
import time
import MySQLdb
import pandas as pd
import numpy as np
import cx_Oracle
from sqlalchemy import types, create_engine,text
from pandas.io import gbq
from google.cloud import bigquery
import os

os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'ukt'
projectid = '78997000000'
client = bigquery.Client(project=PROJECT_ID)
currDate = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")


runFileDir = os.path.dirname(__file__)
pathPass = os.path.join(runFileDir, os.path.normpath(r'../pass/MySQL_db_connect.json'))

with open(pathPass) as f:
    param_сonnect = json.load(f)

conn = create_engine(f'oracle+cx_oracle://{param_сonnect["userOracleProd"]}:{param_сonnect["passwdOracleProd"]}@srv-dwh-prod.h.ru/?service_name=DWH?encoding=utf8')


ip_list = ['46.48.117.x', '91.210.112.x']
domain_list = ['www.ukt123.ru', 'otp1.ukt.ru']

sql_banner = r"with a as (" \
             "SELECT date, hp.PROMOID as PROMO_ID, hp.PROMONAME as PROMO_NAME, hp.PROMOCREATIVE as PROMO_CREATIVE, hits.page.hostname as HOST, hd.value as SITETYPE, count(hits.hitid) as VIEWS " \
             f"from `ukt.owoxbi_sessions_{currDate}` as t, unnest(hits) as hits, unnest(hits.promotion) as hp, unnest (hits.customDimensions) as hd " \
             "where hits.promotionActionInfo='view' and hd.index =12 " \
             f"and t.device.ip not in unnest({ip_list}) and hits.page.hostname in unnest({domain_list}) " \
             "group by 1,2,3,4,5,6)," \
             "b as (SELECT date, hp.promoId, hp.promoName, hp.promoCreative, hits.page.hostname as host, hd.value as SITETYPE, count(hits.hitid) as CLICKS " \
             f"from `ukt.owoxbi_sessions_{currDate}` as t, unnest(hits) as hits, unnest(hits.promotion) as hp, unnest (hits.customDimensions) as hd " \
             "where hits.promotionActionInfo ='click' and hd.index =12 " \
             f"and t.device.ip not in unnest({ip_list}) and hits.page.hostname in unnest({domain_list}) " \
             "group by 1,2,3,4,5,6)" \
             "select a.*, CLICKS from a " \
             "left join b on (a.date=b.date and a.promo_Id=b.promoId and a.promo_Name=b.promoName and a.promo_Creative=b.promoCreative and a.host=b.host and a.sitetype=b.sitetype) " \
             "where a.PROMO_ID!='Aundefined' and VIEWS>3 " \
             "order by 1,7 desc"

sql_pur = r"with a as (" \
              r"SELECT date, sessionId, clientId, user.id, user.owoxId, hits.eCommerceAction.list as list , hp.productSku as sku, sum(hp.productquantity) as addtocart " \
              rf"FROM `ukt.owoxbi_sessions_{currDate}` as t, unnest(hits) as hits, unnest(hits.product) as hp " \
              r"where hits.eCommerceAction.action_type='add' " \
              r"and t.device.ip not in ('46.48.117.230', '91.210.112.20') and hits.page.hostname in ('www.ukt.ru', 'opt.ukt.ru') " \
              r"and regexp_contains(hits.eCommerceAction.list, '^action_(detail|goods)_\\d+') " \
              r"group by 1,2,3,4,5,6,7), " \
              r"b as (SELECT date, sessionId as pp, clientId as clid, user.id as uid, user.owoxId as oid, hp.productSku, hp.productprice, sum(hp.productQuantity) as kupili " \
              fr"FROM `ukt.owoxbi_sessions_{currDate}` as t, unnest(hits) as hits, unnest(hits.product) as hp " \
              r"where hits.eCommerceAction.action_type='purchase' " \
              r"and t.device.ip not in ('46.48.117.230', '91.210.112.20') and hits.page.hostname in ('www.ukt.ru', 'opt.ukt.ru') " \
              r"group by 1,2,3,4,5,6,7) " \
              r"select a.date, list, regexp_extract(list, '\\d{2,6}') as action_id,a.sku as material_id, a.addtocart, productprice, kupili " \
              r"from a " \
              r"left join b on ((sessionId = pp or clientId=clid or id=uid or owoxId=oid) and a.sku=productSku and a.date=b.date)"


def sql_db_select():
    with open(r'C:\Python\ukt\pass\MySQL_db_connect.json') as f:
        param_сonnect = json.load(f)
    db_connect = MySQLdb.connect(user=param_сonnect['user'], passwd=param_сonnect['passwd'],
                                 host=param_сonnect['host'], charset='cp1251'
                                 )
    sql_query = "SELECT promoaction_id AS ACTION_ID, original_id as MATERIAL_ID, DATE(UPDATED) AS ACTION_DATE " \
                 "FROM ukt_prod.ukt_promoact_good pg, ukt_prod.ITEM i " \
                 f"WHERE i.item_id=pg.item_id AND DATE(updated)='{currDate}'"

    sql_action_banner = f"SELECT SUBSTRING(href,-5) as action_id, id as banner_id, name as action_name, date(CREATEd) AS date " \
                        f"FROM ukt_sess.ukt_bann " \
                        f"WHERE href REGEXP '/action/detail/' AND DATE(created)='{currDate}'"

    df_action_items = pd.read_sql(sql_query, con=db_connect)
    df_action_items.ACTION_DATE = pd.to_datetime(df_action_items.ACTION_DATE, format='%Y-%m-%d')
    df_action_items.ACTION_ID = df_action_items.ACTION_ID.astype(str)

    df_action_banners = pd.read_sql(sql_action_banner, con=db_connect)
    if len(df_action_banners)>0:
        df_action_banners['action_id'] = df_action_banners['action_id'].str.extract('(\d{4,6})')
        df_action_banners.action_id = df_action_banners.action_id.astype(str)
        df_action_banners.banner_id = df_action_banners.banner_id.astype(str)
        df_action_banners.date = pd.to_datetime(df_action_banners.date, format='%Y-%m-%d')

    return df_action_items, df_action_banners 


def action_dataToOracle(df_items, df_banners):

    if len(df_items) > 0:
        dtyp = {c: types.VARCHAR(df_items[c].str.len().max())
                for c in df_items.columns[df_items.dtypes == 'object'].tolist()}
        df_items.to_sql('action_item', conn, if_exists='append', index=False, dtype=dtyp)

    if len(df_banners) > 0:
        dtyp_act = {c: types.VARCHAR(df_banners[c].str.len().max())
                    for c in df_banners.columns[df_banners.dtypes == 'object'].tolist()}
        df_banners.to_sql('banners_action', conn, if_exists='append', index=False, dtype=dtyp_act)


def banner_ActionPurchase():
    df_banner = gbq.read_gbq(sql_banner, projectid, dialect='standard')
    df_pur = gbq.read_gbq(sql_pur, projectid, dialect='standard')

    df_banner.date = pd.to_datetime(df_banner.date, format='%Y-%m-%d')
    df_pur.date = pd.to_datetime(df_pur.date, format='%Y-%m-%d')

    df_banner['CLICKS'].fillna(0, inplace=True)
    df_banner['banner_id'] = df_banner['PROMO_ID']
    df_pur['kupili'].fillna(0, inplace=True)
    df_pur['productprice'].fillna(0, inplace=True)
    # update записей, в которых продаж больше, чем addtocart
    df_pur.loc[df_pur['kupili'] > df_pur['addtocart'], 'kupili'] = df_pur['addtocart']


    df_banner.CLICKS = df_banner.CLICKS.astype(int)
    df_pur.kupili = df_pur.kupili.astype(int)

    dtyp_banner = {c: types.VARCHAR(df_banner[c].str.len().max())
                   for c in df_banner.columns[df_banner.dtypes == 'object'].tolist()}
    dtyp_pur = {c: types.VARCHAR(df_pur[c].str.len().max())
                for c in df_pur.columns[df_pur.dtypes == 'object'].tolist()}

    df_banner.to_sql('banner_ctr', conn, if_exists='append', index=False, dtype=dtyp_banner)
    # мержим BANNER_CTR и BANNERS_ACTION
    conn.execute(text(f"""MERGE INTO BANNER_CTR t1 
                     USING (SELECT distinct * FROM BANNERS_ACTION) t2 
                     ON(t1.BANNER_ID = t2.BANNER_ID ) 
                     WHEN MATCHED THEN UPDATE SET 
                     t1.PROMO_ID = t2.ACTION_ID 
                     where t1."date" = to_date('{currDate}', 'YYYYMMDD') and not regexp_like( t1.BANNER_ID, 'A')""").execution_options(autocommit=True))

    # убираем А из PROMO_ID
    conn.execute(f"""update BANNER_CTR set PROMO_ID = regexp_replace(PROMO_ID,'A','') 
                     where "date" = to_date('{currDate}', 'YYYYMMDD') and regexp_like( BANNER_ID, 'A')""")
    conn.execute(f"""delete from BANNER_CTR 
                     where "date" = to_date('{currDate}', 'YYYYMMDD') and PROMO_ID=BANNER_ID and not regexp_like(BANNER_ID, 'A') and VIEWS<10""")


    df_pur.to_sql('action_purchase_items', conn, if_exists='append', index=False, dtype=dtyp_pur)

    # дергаем все акции за currDate
    df_action_id = pd.read_sql_query(f"""select distinct  action_id from ACTION_PURCHASE_ITEMS where "date" = to_date('{currDate}', 'YYYYMMDD')""", conn)

    #Циклом по акциям удаляем все item, которые никогда не были в текущей акции(настоящие товары из акции берем из ACTION_ITEM).
    # Это в основом из-за mobile
    for i in df_action_id['action_id'].values.tolist():
        conn.execute(f"""delete from ACTION_PURCH_ITEM api where not exists(select * from ACTION_ITEM ai where ai.ACTION_ID=api.ACTION_ID and ai.MATERIAL_ID=api.MATERIAL_ID) 
        and api.ACTION_ID='{i}' and api."date" = to_date('{currDate}', 'YYYYMMDD')""")

    # формируем запросом структура таблицы ACTION_PURCHASE из таблицы ACTION_PURCHASE_ITEMS и делаем insert
    conn.execute(f"""insert into ACTION_PURCHASE("date", ACTION_ID, List, ADDTOCART, KUPILI, REVENUE)
                    select "date", ACTION_ID, LIST, sum(ADDTOCART) ADDTOCART, Sum(KUPILI) KUPILI, sum(KUPILI*PRODUCTPRICE) REVENUE 
                    from ACTION_PURCH_ITEM where "date" = to_date('{currDate}', 'YYYYMMDD') 
                    group by "date", ACTION_ID, LIST""")

def main():
    action_items, action_banners = sql_db_select()
    action_dataToOracle(action_items,action_banners)

    table_id = 'owoxbi_sessions_' + currDate
    dataset_ref = client.dataset(DATASET_ID)
    table_ref = dataset_ref.table(table_id)
    while True:
        try:
            client.get_table(table_ref)
            break
        except:
            print('таблица не готова!')
            time.sleep(100)
    banner_ActionPurchase()


if __name__ == '__main__':
        main()