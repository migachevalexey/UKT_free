import datetime
import json
import pprint
import pandas_oracle.tools as pt
import pandas as pd
import numpy as np
import cx_Oracle
from sqlalchemy import types, create_engine
from pandas.io import gbq
from google.cloud import bigquery
import os
from threading import Thread
import smtplib

'''
Тянем данные из потока amplitude BQ и отправляем их в Oracle на ежедневной основе.
2 таблицы: events и events_properties. Таблицу events_properties отправляем пачками, т.к. очень большая
Загрузка данных в таблицы выполняется параллельно. 
Помним, что данные в amplitude BQ тоже отправляются скриптом(вытягиваем их из сервиса Amplitude) и появляются они там в 10:30 по мск 
Если загрузка завершилась ошибкой - отправляем сообщение на email
'''

os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'ukt'
projectid = '78997000000'
client = bigquery.Client(project=PROJECT_ID)

start_time = datetime.datetime.now()
print(start_time, 'Start')
runFileDir = os.path.dirname(__file__)
pathPass = os.path.join(runFileDir, os.path.normpath(r'../pass/MySQL_db_connect.json'))

with open(pathPass) as f:
    param_сonnect = json.load(f)

conn = create_engine(f'oracle+cx_oracle://{param_сonnect["UserDWHStage"]}:{param_сonnect["passDWHStage"]}@srv-dwh-prod.h.ru/?service_name=DWH?encoding=utf8')
passMail = param_сonnect["pass_mail"]
currDate = (datetime.date.today() - datetime.timedelta(days=5)).strftime("%Y-%m-%d")

sql_events = fr"select DATE(_PARTITIONTIME) as date, * FROM `amplitude.events` WHERE DATE(_PARTITIONTIME) = '{currDate}'"
sql_events_prop = f"select *,  DATE(_PARTITIONTIME) as date FROM `amplitude.events_properties` WHERE DATE(_PARTITIONTIME) = '{currDate}'"


def sendMail(tema, text):

    HOST = "smtp.yandex.ru"
    SUBJECT = tema
    TO = "mig@ukt.ru"
    FROM = "mapl333@yandex.ru"
    BODY = "\r\n".join((
        f"From: {FROM}",
        f"To: {TO}",
        f"Subject: {SUBJECT}",
        "", text))
    server = smtplib.SMTP_SSL(HOST, 465)
    server.login('mapl333@yandex.ru', passMail)
    server.sendmail(FROM, [TO], BODY)
    server.quit()


def sendEvents():

    df_events = gbq.read_gbq(sql_events, projectid, dialect='standard')
    df_events.date = pd.to_datetime(df_events.date, format='%Y-%m-%d')
    dtyp = {c: types.VARCHAR(df_events[c].str.len().max())
                   for c in df_events.columns[df_events.dtypes == 'object'].tolist()}
    # conn.execute("""ALTER INDEX AMPLITUDE_EVENTS_DATE_INDEX INVISIBLE""")
    # conn.execute("""ALTER INDEX AMPLITUDE_EVENTS_INSERT_ID_INDEX INVISIBLE""")

    for i in range(0, len(df_events), 100000):
        df_events.iloc[i:i + 100000, :].to_sql('amplitude_events', conn, if_exists='append', index=False,
                                               dtype=dtyp)

    # conn.execute("""ALTER INDEX AMPLITUDE_EVENTS_DATE_INDEX VISIBLE""")
    # conn.execute("""ALTER INDEX AMPLITUDE_EVENTS_INSERT_ID_INDEX VISIBLE""")
    sendMail(f'Load data from Amplidude Events za {currDate} is TRUE',
             f'Insert {str(len(df_events))} records to table AMPLITUDE_EVENTS')

def sendEventsProp(n):

    df_events_prop = gbq.read_gbq(sql_events_prop, projectid, dialect='standard')
    dtyp = {c: types.VARCHAR(df_events_prop[c].str.len().max())
               for c in df_events_prop.columns[df_events_prop.dtypes == 'object'].tolist()}

    for i in range(0, len(df_events_prop), n):
        df_events_prop.iloc[i:i + n, :].to_sql('amplitude_events_properties', conn, if_exists='append', index=False,
                                               dtype=dtyp)
    sendMail(f'Load data from Amplidude Event Properties za {currDate} is TRUE',
             f'Insert {str(len(df_events_prop))} records to table AMPLITUDE_EVENTS_PROPERTIES')


def main():
    try:
        # thread1 = Thread(target=sendEvents)
        thread2 = Thread(target=sendEventsProp, args=(150000,))

        # thread1.start()
        thread2.start()
        # thread1.join()
        thread2.join()

        print(datetime.datetime.now()-start_time, 'Stop')

    except Exception as e:
        sendMail(f"ERROR: Zagruzka Amplidude to Oracle ot {currDate} NE proshla", str(e).encode('utf-8'))

if __name__ == '__main__':
    main()