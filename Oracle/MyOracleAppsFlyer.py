from datetime import date,timedelta,datetime
import json
import smtplib
import pandas_oracle.tools as pt
import pandas as pd
import cx_Oracle
from sqlalchemy import types, create_engine
from pandas.io import gbq
from google.cloud import bigquery
import os
import logging
import logging.handlers


"""Забираем данные из стриминга AppsFlyer. Android и iOS - это две разные таблицы с разной структурой в GBQ
Структура таблиц различается не сильно
Объединяем данные в одну таблицу и отправляем ее в Oracle. 
"""
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
pd.set_option('display.max_rows', 1000)  # для вывода на  печать полного dataFrame
pd.set_option('display.max_columns', 80)
pd.set_option('display.width', 300)

start_time = datetime.now()

PROJECT_ID = 'konic-progress-196909'
projectid = '78997000000'
client = bigquery.Client(project=PROJECT_ID)

runFileDir = os.path.dirname(__file__)
pathPass = os.path.join(runFileDir, os.path.normpath(r'../pass/MySQL_db_connect.json'))

with open(pathPass) as f:
    param_сonnect = json.load(f)

conn = create_engine(
    f'oracle+cx_oracle://{param_сonnect["UserDWHStage"]}:{param_сonnect["passDWHStage"]}@srv-dwh-prod.h.ru/?service_name=DWH?encoding=utf8')
passMail = param_сonnect["pass_mail"]
currDate = (date.today() - timedelta(days=120)).strftime("%Y-%m-%d")
currDate1 = (date.today() - timedelta(days=113)).strftime("%Y-%m-%d")


def sendMail(tema,txt):
    HOST = "smtp.yandex.ru"
    SUBJECT = tema
    TO = "mig@ukt.ru"
    FROM = "mapl333@yandex.ru"
    text = txt
    BODY = "\r\n".join((
        f"From: {FROM}",
        f"To: {TO}",
        f"Subject: {SUBJECT}",
        "", text))

    server = smtplib.SMTP_SSL(HOST, 465)
    server.login('mapl333@yandex.ru', passMail)
    server.sendmail(FROM, [TO], BODY)
    server.quit()


def dataFromBQ():
    sql_and = fr"select * , DATE(_PARTITIONTIME) as date FROM `AppsFlyer.android_events` WHERE DATE(_PARTITIONTIME) between '{currDate}' and '{currDate1}'"
    sql_ios = f"select *, DATE(_PARTITIONTIME) as date FROM `AppsFlyer.ios_events`  WHERE DATE(_PARTITIONTIME)  between '{currDate}' and '{currDate1}'"

    df_and = gbq.read_gbq(sql_and, projectid, dialect='standard')
    df_ios = gbq.read_gbq(sql_ios, projectid, dialect='standard')

    df_and = df_and.reindex(columns=df_and.columns.tolist() + ['device_name'])
    df_ios = df_ios.reindex(
        columns=df_ios.columns.tolist() + ['operator', 'android_id', 'device_brand', 'validated', 'advertising_id',
                                           'carrier', 'device_model', 'IMEI'])
    df_ios = df_ios[df_and.columns.tolist()]

    FinalDF = pd.concat([df_and, df_ios], sort=False)

    FinalDF[['click_time', 'attributed_touch_time', 'download_time', 'install_time', 'event_time',
             'download_time_selected_timezone', 'click_time_selected_timezone', 'install_time_selected_timezone',
             'event_time_selected_timezone', 'date']] = \
        FinalDF[['click_time', 'attributed_touch_time', 'download_time', 'install_time', 'event_time',
                 'download_time_selected_timezone', 'click_time_selected_timezone', 'install_time_selected_timezone',
                 'event_time_selected_timezone', 'date']].apply(pd.to_datetime)
    # убираем символы " и af_ - они дают сильный прирост в длине поле event_value
    FinalDF['event_value'] = FinalDF['event_value'].str.replace('"|af_', '', regex=True)

    return FinalDF


def dataToOracle(n):
    dataBQ = dataFromBQ()

    dtyp = {c: types.VARCHAR(dataBQ[c].astype(str).str.len().max())
            for c in dataBQ.columns[dataBQ.dtypes == 'object'].tolist()}

    # если len(event_value)>4000(бывает при больших заказах), тогда берем только первые 4000
    dataBQ['event_value'] = dataBQ['event_value'].map(
        lambda x: x[:4000] if (x is not None and len(x) > 4000) else x)

    for i in range(0, len(dataBQ), n):
        try:
            dataBQ.iloc[i:i + n, :].to_sql('appsflyer', conn, if_exists='append', index=False, dtype=dtyp)
        except:
            z = dataBQ.iloc[i:i + n, :]
            m = 5
            for j in range(0, len(z), m):
                try:
                    z.iloc[j:j+m, :].to_sql('appsflyer', conn, if_exists='append', index=False, dtype=dtyp)
                except:
                    z.iloc[j:j+m, :].to_csv(f'exceptionAppsflyer{currDate}_{i}.csv', index=False)
                    continue

    sendMail(f'Load data from AppsFlyer za {currDate} is True',f'Insert {str(len(dataBQ))} records from AppsFlyer')


def main():

    try:
        dataToOracle(50000)
    except Exception as e:
        sendMail(f"ERROR: Zagruzka AppsFlyer to Oracle ot {currDate} NE proshla", f"ERROR: {str(e).encode('utf-8')}")
        conn.execute(f"""delete from APPSFLYER where "date" = to_date('{currDate}', 'YYYY-MM-DD')""")


if __name__ == '__main__':
    main()
