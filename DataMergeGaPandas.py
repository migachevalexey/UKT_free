"""Analytics Reporting API V4."""
import datetime
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import pandas as pd
import re
from pandas import ExcelWriter

"""
Если нужно вытянуть более 9 параметров из GA по транзакции
Тянем двумя запросаи и через pd.merge соединяем их по ключу transactionId
Слабое место - в результате количество строк МОЖЕТ РАЗЛИЧАТЬСЯ в разных запросах!
Это происходит из-за того, что каких то параметров во втором запросе нет в GA
Merge делаем меньшего к большему через left 
"""

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
SERVICE_ACCOUNT_EMAIL = 'ukt-cloud@konic-progress-196909.iam.gserviceaccount.com'
DISCOVERY_URI = 'https://analyticsreporting.googleapis.com/$discovery/rest'
KEY_FILE_LOCATION = r'C:\Python\ukt\pass\uktOwox-d00000000e12.p12'
VIEW_ID = '115850000'  # id нужного представления в GA


def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_p12_keyfile(
        SERVICE_ACCOUNT_EMAIL, KEY_FILE_LOCATION, scopes=SCOPES)
    http = credentials.authorize(httplib2.Http())
    analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)

    return analytics

def get_report(analytics, date):
    # help -> https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet?hl=ru
    # 'pageSize' - сколько записей выводим, pageToken : "10001" - начиная с какой записи выводить, если запрос содержит больше 10т

    QUERY = {
        'reportRequests': [
            {'viewId': VIEW_ID,
             'dateRanges': [{'startDate': date, 'endDate': date}],
             "samplingLevel": 'LARGE',
             "dimensions": [
                 {"name": "ga:landingPagePath"},
             ],
             'metrics': [{"expression": "ga:sessions"}, ],
             "dimensionFilterClauses": [{
                 "filters": [
                     {"dimensionName": "ga:landingPagePath",
                      "expressions": [
                          #"/253-479-452200$|/193-479-453181$|/2486-479-452284$|/816-479-452950$|/193-479-912131$|/251-479-452307$|/253-479-452278$|/253-479-453583$|/692-479-452272$$"],
                          "^/item/194/1003342|/item/194/1003347|/item/252/1003753|/item/252/1003755"],
                      "operator": "REGEXP"
                      },
                 ]}],
             'pageSize': 10000}
        ]}

    request = analytics.reports().batchGet(body=QUERY).execute()  # запрос API
    rowCount = request['reports'][0]['data']['rowCount']
    API_data = request['reports'][0]['data']['rows']
    if rowCount > 10000:  # если записей болше 10т, упираемся в лимит и делаем цикл с параметром nextPageToken
        for i in range(rowCount // 10000):
            pageToken = request['reports'][0]["nextPageToken"]
            QUERY['reportRequests'][0].update({"pageToken": pageToken})  # добавляем параметр pageToken в dict запроса
            request = analytics.reports().batchGet(body=QUERY).execute()  # посылаем запрос API
            API_data += request['reports'][0]['data']['rows']

    print(f'Колво записей: {rowCount}')

    API_data = [list(i['dimensions']) + list(i['metrics'][0]['values']) for i in API_data]
    print(len(API_data))
    return API_data


def main():
    a = {}
    analytics = initialize_analyticsreporting()
    dd = pd.read_csv('121.csv', header=0, sep=';', encoding='cp1251', names=['sku', 'name', 'url'])
    for i in range(14,0,-1):
        currDate = (datetime.date.today() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        z = get_report(analytics, currDate)
        for j in z:
            a[re.sub('\\?.*', '', j[0])] = int(a.get(j[0], 0)) + int(j[1])

        d1 = pd.DataFrame(list(a.items()), columns=['url', currDate])
        dd = pd.merge(dd, d1, how='left', on=['url'])
        dd[currDate]=dd[currDate].fillna(0)
        dd.fillna(0, axis=1)
        a.clear()

    writer = ExcelWriter(r'\\ANALYTICS\WEB-ANALYTICS\Goods_tracking\Внешний трафик(SKU).xlsx')
    dd.to_excel(writer, 'Sheet1', index=False)
    writer.save()

if __name__ == '__main__':
    main()