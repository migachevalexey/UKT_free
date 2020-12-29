import datetime
import json
import pandas as pd
from pandas.io import gbq
from sqlalchemy import types, create_engine
import os
import MySQLdb
from google.cloud import bigquery
from threading import Thread

os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'ukt'
projectid = '78997000000'
client = bigquery.Client(project=PROJECT_ID)

# runFileDir = os.path.dirname(__file__)
# pathPass = os.path.join(runFileDir, os.path.normpath(r'../pass/MySQL_db_connect.json'))
# 
# with open(pathPass) as f:
#     param_сonnect = json.load(f)
# 
# conn = create_engine(f'oracle+cx_oracle://{param_сonnect["userOracleProd"]}:{param_сonnect["passwdOracleProd"]}@srv-dwh-pr
start_time=datetime.datetime.now()

sql_sess_id = """SELECT 
    distinct
      date,
      TIMESTAMP_MILLIS(safe_cast(substr(sessionId, -13, 13) as int64)) as utc_time,
      FORMAT_TIMESTAMP("%H:%M:%S", TIMESTAMP_MILLIS(safe_cast(substr(sessionId, -13, 13) as int64)), "Europe/Moscow") as time,
      sessionId,
      user.id as userID,
      user.owoxId,
      clientId,
      landingPage,
      visitNumber,
      newVisits,
      t.device.* EXCEPT(userAgent,LANGUAGE),
      geoNetwork.* EXCEPT(countryIsoCode,regionIsoCode,latitude,longitude),
      trafficSource.source,
      trafficSource.medium,
      trafficSource.campaign,
      trafficSource.keyword,
      trafficSource.adContent,
      trafficSource.isTrueDirect
    FROM
      `konic-progress-196909.ukt.owoxbi_sessions_{}` AS t, UNNEST(hits) AS hits
    WHERE
       t.device.ip !='46.48.117.230' AND hits.page.hostname IN ('www.ukt.ru','opt.ukt.ru')
    ORDER BY 1,2"""

sql_sess_hits = """SELECT distinct
      date,
      safe_cast(substr(sessionId, -13, 13) as int64) as millis,
      sessionId,
      user.id as userID,
      user.owoxId,
      clientId,
      hits.hitId,
      hits.isEntrance,
      hits.isExit,
      hits.type
    FROM
      `konic-progress-196909.ukt.owoxbi_sessions_{}` AS t,   UNNEST(hits) AS hits
    WHERE
      t.device.ip !='46.48.117.230' AND hits.page.hostname IN ('www.ukt.ru','opt.ukt.ru')
    ORDER BY 4,3"""

sql_hits = """SELECT distinct
      date,
      safe_cast(substr(sessionId, -13, 13) as int64) as millis,
      hits.hitId,
      hits.type,
      REGEXP_EXTRACT(hits.referer,'^([^?]+)?') AS reffer,
      hits.device.screenResolution,
      hits.page.pageType,
      REGEXP_EXTRACT(hits.page.pagePath,'^([^?]+)?') AS pagePath,
      hits.page.hostname,
      hits.page.pageTitle,
      hits.eCommerceAction.*,
      hits.transaction.* EXCEPT(localTransactionRevenue,localTransactionTax,localTransactionShipping),
      hp.productSku,
      hp.productPrice,
      hp.productQuantity,
      hp.position,
      hits.eventInfo.* EXCEPT(eventValue)
    FROM
      `konic-progress-196909.ukt.owoxbi_sessions_{}` AS t,   UNNEST(hits) AS hits,
       UNNEST(hits.product) AS hp
    WHERE
      t.device.ip !='46.48.117.230' AND hits.page.hostname IN ('www.ukt.ru','opt.ukt.ru')"""

sql_hits_demension = """SELECT 
      date,
      safe_cast(substr(sessionId, -13, 13) as int64) as MILLIS,
      hits.hitId,
      hits.type,
      hd.index,
      hd.value
    FROM
      `konic-progress-196909.ukt.owoxbi_sessions_{}` AS t, UNNEST(hits) AS hits,
       UNNEST(hits.customDimensions) AS hd
    WHERE
       t.device.ip !='46.48.117.230' AND hits.page.hostname IN ('www.ukt.ru','opt.ukt.ru') AND hd.index IN (2,10,12,14,15,16,19)
    ORDER BY 4,3"""



def OwoxData(sql,tableName,date, param=None):
    n=10**5
    df = gbq.read_gbq(sql.format(date), projectid, dialect='standard')
    df.date = pd.to_datetime(df.date, format='%Y-%m-%d')
    dtyp = {c: types.VARCHAR(df[c].astype(str).str.len().max())
            for c in df.columns[df.dtypes == 'object'].tolist()}
    # Если keyword более 4000 символов, обрезаем его
    if param in df.columns:
        df[param] = df[param].map(
            lambda x: x[:4000] if (x is not None and len(x) > 4000) else x)
    for i in range(0, len(df), n):
        df.iloc[i:i + n, :].to_sql(tableName,conn,if_exists='append',index=False,schema='DWH',dtype=dtyp)
    print(f'Count {tableName} -', len(df))

def main ():
    date_list = [(start_time - datetime.timedelta(days=x)).strftime("%Y%m%d") for x in range(1,2)]
    for i in date_list:
        thread1 = Thread(target=OwoxData, args=(sql_sess_id,'owox_session_id',i,), kwargs={'param':'keyword'})
        thread2 = Thread(target=OwoxData, args=(sql_sess_hits,'owox_session_hits',i,))
        thread3 = Thread(target=OwoxData, args=(sql_hits,'owox_hits',i,), kwargs={'param':'pagePath'})
        thread4 = Thread(target=OwoxData, args=(sql_hits_demension,'owox_hits_demension',i,))

        thread1.start()
        thread2.start()
        thread3.start()
        thread4.start()
        thread1.join()
        thread2.join()
        thread3.join()
        thread4.join()

main()
print(datetime.datetime.now() - start_time)
