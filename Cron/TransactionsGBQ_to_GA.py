"""Analytics Reporting API V4."""
import csv
import pprint
import datetime
from datetime import datetime as ddatetime
import time
import urllib3
import requests
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
from google.cloud import bigquery
from pandas.io import gbq
from collections import defaultdict
import smtplib

projectid = '78997000000'

"""
Данный скрипт тянет заказы(которых нет в GA) их хитового стриминга OWOX и отправляет эти заказы в GA(UA-8) через Measurement Protocol
Заказы тянутся вместе с товарами (id, количество, цена)
Из таблиц сессионного стриминга в заказ тянуться все нужные параметры - источник, канал, кампания и т.д.  в словаре payload подробное описание.
В случае большого расхождения по заказам - отправляем письмо на email и отправляем первые 150 заказов. 
Передача всех данных логируется.
"""

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
SERVICE_ACCOUNT_EMAIL = 'ukt-cloud@konic-progress-196909.iam.gserviceaccount.com'
DISCOVERY_URI = 'https://analyticsreporting.googleapis.com/$discovery/rest'
KEY_FILE_LOCATION = r'../pass/uktOwox-d00000000e12.p12'
VIEW_ID = '115850000'  # представление RawData ukt(GTM)
DATASET_ID = 'ukt'
PROJECT_ID = 'konic-progress-196909'

urllib3.disable_warnings()  # отключаем предупреждения(Warnings) всякие

currDateGA = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
currDate = currDateGA.replace('-', '')
currMonth = currDateGA[:7]
table_id = 'owoxbi_sessions_' + currDate

client = bigquery.Client(project=PROJECT_ID)
dataset = client.dataset(DATASET_ID)

with open(r'../pass/mail.txt') as f:
    p=f.readline()


def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_p12_keyfile(
        SERVICE_ACCOUNT_EMAIL, KEY_FILE_LOCATION, scopes=SCOPES)
    http = credentials.authorize(httplib2.Http())
    analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)

    return analytics


F_body = {
    'reportRequests': [
        {'viewId': VIEW_ID,
         'dateRanges': [{'startDate': f'{currDateGA}', 'endDate': f'{currDateGA}'}],
         "samplingLevel": 'LARGE',
         "dimensions": [{"name": "ga:transactionId"}],
         'metrics': [{"expression": "ga:transactionRevenue"} ],  # {"expression": "ga:transactionShipping"}
         'pageSize': 10000}
    ]}


def transactionGA(analytics, QUERY):
    # help -> https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet?hl=ru
    # 'pageSize' - сколько записей выводим, pageToken : "10001" - начиная с какой записи выводить, если запрос содержит больше 10т

    request = analytics.reports().batchGet(body=QUERY).execute()  # запрос API
    rowCount = request['reports'][0]['data']['rowCount']
    API_data = request['reports'][0]['data']['rows']
    if rowCount > 10000:  # если записей больше 10т, упираемся в лимит и делаем цикл с параметром nextPageToken
        for i in range(rowCount // 10000):
            pageToken = request['reports'][0]["nextPageToken"]
            QUERY['reportRequests'][0].update({"pageToken": pageToken})  # добавляем параметр pageToken в dict запроса
            request = analytics.reports().batchGet(body=QUERY).execute()  # посылаем запрос API
            API_data += request['reports'][0]['data']['rows']

    print(f'Колво записей в GA: {rowCount}')
    API_data = [i['dimensions'][0] for i in API_data]

    return set(API_data)


def transactionGBQ():
    QUERY = "SELECT distinct transaction.transactionId FROM `konic-progress-196909.ukt.streaming_{}` as t " \
            "where transaction.transactionId is not null and eCommerceAction.action_type='purchase' and  t.device.ip !='46.48.117.230' " \
            "and page.hostname in ('www.ukt.ru','opt.ukt.ru')".format(currDate)

    # по старенькому )
    # query_job = client.query(QUERY)  # API request
    # rows = query_job.result()  # Waits for query to finish
    # z=[row[0] for row in rows]

    data_frame = gbq.read_gbq(QUERY, projectid, dialect='standard')
    set_transaction = set(data_frame['transactionId'].unique())
    print(f'Колво записей в BQ: {len(set_transaction)}')
    return set_transaction

# Есть в GA нет в GBQ и наоборот
def differenceTransaction():
    analytics = initialize_analyticsreporting()
    GA_Tran = transactionGA(analytics, F_body)
    GBQ_Tran = transactionGBQ()
    print('Есть в GA, но нет в GBQ: ', len(GA_Tran-GBQ_Tran), GA_Tran-GBQ_Tran)
    print('Есть в BQ, но нет в GA: ', len(GBQ_Tran-GA_Tran))
    print('Недостающие в GA: ', ','.join(GBQ_Tran-GA_Tran))
    with open(r'./log/MP/BQvsGA_Transaction.txt', 'a') as f:
        f.write('На дату: '+currDateGA+'\n'+
                'Всего в GA: '+str(len(GA_Tran))+'\n'+
                'Всего в GBQ: '+str(len(GBQ_Tran))+'\n'+
                'Есть в GA, но нет в GBQ: '+str(len(GA_Tran-GBQ_Tran))+'\n'+
                'Есть в BQ, но нет в GA: '+str(len(GBQ_Tran-GA_Tran))+'\n'+
                 ','.join(GBQ_Tran-GA_Tran)+'\n'+'-'*65+'\n')

    return list(GBQ_Tran-GA_Tran)

def sendMail(l):

    HOST = "smtp.yandex.ru"
    SUBJECT = f"Zagruzka ot {currDateGA} proshla NE polnostiu"
    TO = "mig@ukt.ru"
    FROM = "mapl333@yandex.ru"
    text = f"Zagruzka zakazov ot {currDateGA} prosha NE polnostiu. Raznitca mezhdu BQ i GA prevyshaet 100 zakazov. Kolvo zakazov = {l}. Zagrugeno 100 zakazov"
    BODY = "\r\n".join((
        f"From: {FROM}",
        f"To: {TO}",
        f"Subject: {SUBJECT}",
        "", text))

    server = smtplib.SMTP_SSL(HOST, 465)
    server.login('mapl333@yandex.ru', p)
    server.sendmail(FROM, [TO], BODY)
    server.quit()

# Тянем источник\канал и товары по недостающим транзакциям из сесионного стримига OWOXBI
# Cложность в том, что таблица owoxbi_sessions_  формируется в разное время, ожидание реализовано через While
def lostTransaction():
    z = differenceTransaction()
    if len(z) > 150:
        sendMail(len(z))
    dataset_ref = client.dataset(DATASET_ID)
    table_ref = dataset_ref.table(table_id)

    while True:
        try:
            client.get_table(table_ref)
            break
        except:
            time.sleep(300)

    query_orders = 'select distinct hits.transaction.transactionId, clientId, user.id, t.device.ip,' \
                   'hits.transaction.transactionRevenue, hits.transaction.transactionShipping, hits.transaction.affiliation,' \
                   'trafficSource.source, trafficSource.medium, trafficSource.campaign,t.device.userAgent,dimension12.value as cd12, dimension5.value as cd5,dimension20.value as cd20 ' \
                   'from `ukt.owoxbi_sessions_{}` as t, unnest(hits) as hits, unnest(hits.customDimensions) as dimension12, unnest(hits.customDimensions) as dimension5,unnest(hits.customDimensions) as dimension20 ' \
                   'where hits.transaction.transactionId in UNNEST({}) and hits.eCommerceAction.action_type="purchase" and dimension5.index=5 and  dimension12.index=12 and dimension20.index=20'.format(currDate, z)

    query_items = 'select hits.transaction.transactionId, hp.productSku, hp.productPrice, hp.productQuantity ' \
                  'from `ukt.owoxbi_sessions_{}` as t, unnest(hits) as hits, unnest(hits.product) as hp ' \
                  'where hits.transaction.transactionId in UNNEST({}) and  hits.eCommerceAction.action_type="purchase"'.format(currDate, z)

    query_job = client.query(query_orders)  # API request
    rows = query_job.result()  # Waits for query to finish
    listSourceTransactions = [[row[i] for i in range(14)] for row in rows]
    if len(listSourceTransactions) > 150:
        listSourceTransactions=listSourceTransactions[:150]

    orderItemsList = defaultdict(list)

    query_job = client.query(query_items)  # API request
    rowsItems = query_job.result()  # Waits for query to finish
    OrderItems = [[row[i] for i in range(4)] for row in rowsItems]

    for row in OrderItems:
        key, val = row[0], [row[1], row[2], row[3]]
        orderItemsList[key].append(val)

    with open(r'./log/MP/{}/LostSource_{}.csv'.format(currMonth, currDate), "w", newline='') as f, \
         open(r'./log/MP/{}/OrderItems_{}.csv'.format(currMonth, currDate), "w", newline='') as f1:
        writer = csv.writer(f, delimiter=';')
        writer.writerows(listSourceTransactions)
        writer = csv.writer(f1, delimiter=';')
        writer.writerows(OrderItems)

    return listSourceTransactions, orderItemsList

def MpOrdesToGA():
    endpoint = 'https://www.google-analytics.com/collect'
    orders, orderItems = lostTransaction()
    x = {}
    with open(r'./log/MP/{}/lostTransactionToGA_{}.txt'.format(currMonth,currDate), 'a') as file:
        file.write(currDateGA+' '+ddatetime.now().time().strftime("%H.%M") +'\n')
        for i in orders:
                payload = {
                   'v': '1',
                   'tid': 'UA-8149186-8',
                   't': 'pageview',
                   'cid': i[1],     # ClientId
                   'ua': i[10],     # userAgent типа - Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit....  нужен для браузера
                   'ti': i[0],      # Id транзакции
                   'dp': '/ordering/thanks',
                   'ta': i[6],  # партнер(affiliation)
                   'tr': i[4],  # Revenue
                   'ts': i[5],  # Shipping
                   'pa': 'purchase',
                   'uid': i[2],     # userID
                   'qt': 14100000,  # 3 часа 55 мин
                   'uip': i[3], # IP пользователя
                   'cm': i[8],  # канал
                   'cs': i[7],  # источник
                   'cn': i[9],  # кампания
                   'cd12': i[11],   # siteType - специальный параметр
                   'cd5': i[12],  # userType - специальный параметр
                   'cd20': i[13],  # AB_test_status (сегменты A,D,C..) - специальный параметр
                   'ni': 1      # non-Iteraction
                    }
                s = orderItems[i[0]]
                # тут формируем dict c товарами транзакции - Id товара, цена, количество.
                for j, val in enumerate(s, start=1):
                    x.update({f'pr{j}id': val[0],
                              f'pr{j}pr': val[1],
                              f'pr{j}qt': val[2]})

                payload.update(x)  # объединяем словари заказ+содержимое заказа
                r = requests.post(url=endpoint, data=payload,
                                  headers={'User-Agent': i[10]},
                                  verify=False)
                file.write(i[0] + ' ' + str(r.status_code) + '\n')
                time.sleep(1)
                payload.clear()
                x.clear()
        file.write('Отправка прошла успешно!')


def main():
    MpOrdesToGA()

if __name__ == '__main__':
    main()