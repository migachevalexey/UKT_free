"""Analytics Reporting API V4."""
import datetime

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import pandas as pd
from pandas.io import gbq

"""
Если нужно вытянуть более 9 параметров из GA по транзакции
Тянем двумя запросаи и через pd.merge соединяем их по ключу transactionId
в некоторых заказах отдельные параметры могут отсутствовать! 
"""

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
SERVICE_ACCOUNT_EMAIL = 'ukt-cloud@konic-progress-196909.iam.gserviceaccount.com'
DISCOVERY_URI = 'https://analyticsreporting.googleapis.com/$discovery/rest'
KEY_FILE_LOCATION = r'C:\Python\ukt\pass\uktOwox-d00000000e12.p12'
VIEW_ID = '115850000'  # id нужного представления в GA

# date_start = str(datetime.date.today() - datetime.timedelta(days=3))
# date_stop = date_start
date_start = '2019-03-01'
date_stop = '2019-03-01'


def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_p12_keyfile(
        SERVICE_ACCOUNT_EMAIL, KEY_FILE_LOCATION, scopes=SCOPES)
    http = credentials.authorize(httplib2.Http())
    analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)

    return analytics


F_body = {
    'reportRequests': [
        {'viewId': VIEW_ID,
         'dateRanges': [{'startDate': f'{date_start}', 'endDate': f'{date_stop}'}],
         "samplingLevel": 'LARGE',
         "dimensions": [{"name": "ga:transactionId"},
                        {"name": "ga:hostname"},
                        {'name': 'ga:channelGrouping'},
                        {"name": "ga:medium"}, {"name": "ga:source"},
                        {"name": "ga:adwordsCampaignID"}, {"name": "ga:campaign"}, {'name': 'ga:keyword'},
                        {"name": "ga:deviceCategory"}],
         'metrics': [{"expression": "ga:transactionRevenue"}, ],  # {"expression": "ga:transactionShipping"}
         'pageSize': 10000}
    ]}

S_body = {
    'reportRequests': [
        {'viewId': VIEW_ID,
         'dateRanges': [{'startDate': f'{date_start}', 'endDate': f'{date_stop}'}],
         "samplingLevel": 'LARGE',
         "dimensions": [{"name": "ga:transactionId"}, {"name": "ga:date"},
                        {'name': 'ga:region'}, {"name": "ga:city"}],
         'metrics': [{"expression": "ga:transactionRevenue"}],
         'pageSize': 10000}]}


def get_report(analytics, QUERY):
    # help -> https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet?hl=ru
    # 'pageSize' - сколько записей выводим, pageToken : "10001" - начиная с какой записи выводить, если запрос содержит больше 10т

    request = analytics.reports().batchGet(body=QUERY).execute()  # запрос API
    rowCount = request['reports'][0]['data']['rowCount']
    # pprint.pprint(zz['reports'][0]['data']['rowCount'])  # - колво  записей
    API_data = request['reports'][0]['data']['rows']
    if rowCount > 10000:  # если записей болше 10т, упираемся в лимит и делаем цикл с параметром nextPageToken
        for i in range(rowCount // 10000):
            pageToken = request['reports'][0]["nextPageToken"]
            QUERY['reportRequests'][0].update({"pageToken": pageToken})  # добавляем параметр pageToken в dict запроса
            request = analytics.reports().batchGet(body=QUERY).execute()  # посылаем запрос API
            API_data += request['reports'][0]['data']['rows']

    print(f'Колво записей: {rowCount}')

    API_data = [list(i['dimensions']) + list(i['metrics'][0]['values']) for i in API_data]

    return API_data


def fin_obrabotchik(analytics):
    primary_data = get_report(analytics, F_body)
    secondary_data = get_report(analytics, S_body)
    F_labels = ['transactionId', 'hostname', 'channelGrouping', 'medium', 'source', 'adwordsCampaignID', 'campaign',
                'keyword', 'device', 'transactionRevenue']
    S_labels = ['transactionId', 'date', 'region', 'city', 'Revenue']
    F_df = pd.DataFrame.from_records(primary_data, columns=F_labels)
    S_df = pd.DataFrame.from_records(secondary_data, columns=S_labels)
    S_df.drop('Revenue', axis=1)  # удаляем столбец 'Revenue', т.к. он дублируется

    finalData = pd.merge(F_df, S_df, on=['transactionId'])
    finalData = finalData[
        ['transactionId', 'date', 'transactionRevenue', 'channelGrouping', 'medium', 'source',
         'adwordsCampaignID', 'campaign', 'keyword', 'device', 'region', 'city',
         'hostname']]  # определяем порядок столбцов

    return finalData


def stream_dataToBQ(ins_data):
    # BigQuery params
    PROJECT_ID = '78997000000'
    DATASET_ID = 'Temp'
    TABLE_ID = 'OrderPandasPartit'
    import_action = 'append'
    gbq.to_gbq(ins_data, f'{DATASET_ID}.{TABLE_ID}', PROJECT_ID, if_exists=import_action)


def main():
    analytics = initialize_analyticsreporting()
    z = fin_obrabotchik(analytics)
    stream_dataToBQ(z)


if __name__ == '__main__':
    main()