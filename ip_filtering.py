import smtplib
import json
import time
import os
import requests
import pandas as pd
from pandas.io import gbq
from google.cloud import bigquery
import datetime


PROJECT_ID = 'konic-progress-196909'
projectid = '78997000000'
client = bigquery.Client(project=PROJECT_ID)

runFileDir = os.path.dirname(__file__)
pathMail = os.path.join(runFileDir, os.path.normpath(r'./pass/mail.txt'))
pathIP_Filter = os.path.join(runFileDir, os.path.normpath(r'./txtcsvFile/ipFilter.txt'))

with open(pathMail) as f, open(pathIP_Filter) as f_ip:
    p = f.readline()
    content = f_ip.readlines()

ipFilter = [x.strip() for x in content] # you may also want to remove whitespace characters like `\n` at the end of each line

currDate = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")

sql = f"SELECT t.device.ip, count(hits.hitid) FROM ukt.owoxbi_sessions_{currDate} as t, unnest(hits) as hits " \
      rf"where not regexp_contains( t.device.ip, '^91\\.210\\.11[2-5]\\..*|^10\\.8\\.*|^10\\.9\\.1[10]\\.*')  and t.device.ip not in UNNEST({ipFilter}) " \
      "group by 1 " \
      "having count(hits.hitid)>3800"

df = gbq.read_gbq(sql, projectid, dialect='standard')
z = df.values.tolist()

ip_list = []
for i in z:
    url = 'http://ip-api.com/json/' + i[0]
    q = requests.get(url)
    if q.json()['regionName'].find('Moscow') == -1:
        ip_list.append(q.json()['query'])
        print(f"{q.json()['query']}, {q.json()['regionName']}, {q.json()['city']}" + '\n --------------')

dd = dict()
for i in ip_list:
    sql = "SELECT if( regexp_contains(hits.pagePath, 'item'), 'item', if(regexp_contains(hits.pagePath,'/cat/'),'cat',hits.pagePath)) ,count(hits.hitid) " \
          f"FROM `konic-progress-196909.ukt.owoxbi_sessions_{currDate}` as t, unnest(hits) as hits " \
          f"where  t.device.ip ='{i}' and hits.type ='pageview'" \
          "group by 1 order by 2 desc"
    df = gbq.read_gbq(sql, projectid, dialect='standard')
    dd[i] = df.values.tolist()
    print(i, df.values.tolist())
time.sleep(5)


def sendMail():
    HOST = "smtp.yandex.ru"
    SUBJECT = f"IP bots {currDate} "
    TO = "mig@ukt.ru"
    FROM = "mapl333@yandex.ru"
    text = json.dumps(dd, sort_keys=True, indent=4)
    BODY = "\r\n".join((
        f"From: {FROM}",
        f"To: {TO}",
        f"Subject: {SUBJECT}",
        "", text))
    server = smtplib.SMTP_SSL(HOST, 465)
    server.login('mapl333@yandex.ru', p)
    server.sendmail(FROM, [TO], BODY)
    server.quit()


sendMail()
print()
time.sleep(5)
