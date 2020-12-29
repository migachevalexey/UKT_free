import urllib3
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import pandas as pd

urllib3.disable_warnings()

"""
Если нужно вытянуть более 9 параметров из GA по транзакции
Тянем двумя запросаи и через pd.merge соединяем их по ключу transactionId
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


def get_report(analytics, QUERY):

    request = analytics.reports().batchGet(body=QUERY).execute()  # запрос API
    rowCount = request['reports'][0]['data']['rowCount']
    API_data = request['reports'][0]['data']['rows']
    if rowCount > 10000:
        for i in range(rowCount // 10000):
            pageToken = request['reports'][0]["nextPageToken"]
            QUERY['reportRequests'][0].update({"pageToken": pageToken})  # добавляем параметр pageToken в dict запроса
            request = analytics.reports().batchGet(body=QUERY).execute()  # посылаем запрос API
            API_data += request['reports'][0]['data']['rows']

    print(f'Колво записей: {rowCount}')

    API_data = [list(i['dimensions']) for i in API_data]

    return API_data


def fin_obrabotchik(analytics, dstart,dstop):
    F_body = {
        'reportRequests': [
            {'viewId': VIEW_ID,
             'dateRanges': [{'startDate': f'{dstart}', 'endDate': f'{dstop}'}],
             "samplingLevel": 'LARGE',
             "dimensions": [{"name": "ga:transactionId"},
                            {"name": "ga:browser"},
                            {"name": "ga:userType"},
                            {'name': 'ga:channelGrouping'},
                            {"name": "ga:medium"}, {"name": "ga:source"},
                            {"name": "ga:campaign"}, {'name': 'ga:keyword'},
                            {"name": "ga:deviceCategory"}],
             'metrics': [{"expression": "ga:transactionRevenue"}, ],
             'pageSize': 10000}
        ]}

    S_body = {
        'reportRequests': [
            {'viewId': VIEW_ID,
             'dateRanges': [{'startDate': f'{dstart}', 'endDate': f'{dstop}'}],
             "samplingLevel": 'LARGE',
             "dimensions": [{"name": "ga:transactionId"}, {"name": "ga:city"},
                            {"name": "ga:dimension6"}, {'name': 'ga:affiliation'}],
             'metrics': [{"expression": "ga:transactionRevenue"}],
             'pageSize': 10000}]}

    primary_data = get_report(analytics, F_body)
    secondary_data = get_report(analytics, S_body)
    F_labels = ['transaction', 'browser','userTypeGA', 'channel', 'medium', 'source',  'campaign',
                'keyword', 'device']
    S_labels = ['transaction', 'city',  'clientId', 'siteType']
    F_df = pd.DataFrame.from_records(primary_data, columns=F_labels)
    S_df = pd.DataFrame.from_records(secondary_data, columns=S_labels)
    F_df= F_df.drop_duplicates(subset='transaction')
    S_df= S_df.drop_duplicates(subset='transaction')
    print(f'F_df - {len(F_df)}\n', f'S_df - {len(S_df)}')

    finalData = pd.merge(F_df, S_df, how='left', on=['transaction']) # делаем join слева меньшего к большему
    finalData = finalData[
        ['transaction', 'channel', 'medium', 'source', 'campaign',
                'keyword', 'device','siteType', 'browser', 'clientId', 'city','userTypeGA']]  # определяем порядок столбцов
    finalData=finalData.drop_duplicates(subset='transaction')  # на всякий случай.
    print(f'finalData - {len(finalData)}')

    return finalData


def main(date_start, date_stop):
    analytics = initialize_analyticsreporting()
    z = fin_obrabotchik(analytics, date_start, date_stop)
    return z


if __name__ == '__main__':
    main()