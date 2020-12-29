"""Analytics Reporting API V4."""
import csv
import pprint
import numpy as np
import requests
from pandas.io import gbq
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import pandas as pd
import datetime

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
SERVICE_ACCOUNT_EMAIL = 'ukt-cloud@konic-progress-196909.iam.gserviceaccount.com'
DISCOVERY_URI = 'https://analyticsreporting.googleapis.com/$discovery/rest'
KEY_FILE_LOCATION = r'C:\Python\ukt\pass\uktOwox-d00000000e12.p12'
filePath = r'\\srv-\WEB-ANALYTICS\File_transfer\DayDataReports.csv'
VIEW_ID = '115850000'  #id


date_start = (datetime.date.today() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
date_stop = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_p12_keyfile(
        SERVICE_ACCOUNT_EMAIL, KEY_FILE_LOCATION, scopes=SCOPES)
    http = credentials.authorize(httplib2.Http())
    analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)

    return analytics


def get_report(analytics, date_start,date_stop):
    QUERY = {
        'reportRequests': [ # https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet?hl=ru#DimensionFilter
            {'viewId': VIEW_ID,
             'dateRanges': [{'startDate': date_start, 'endDate': date_stop}],
             "samplingLevel": 'LARGE',
             "dimensions": [{"name": "ga:date"}],
             'metrics': [{"expression": "ga:sessions"}],
             'pageSize': 10000}
        ]}

    request = analytics.reports().batchGet(body=QUERY).execute()  # запрос API

    API_data = request['reports'][0]['data']['rows']
    API_data = [list(i['dimensions']) + list(i['metrics'][0]['values']) for i in API_data]
    return API_data


def dataFromAppMetrika():

    params_sess = {'lang': 'ru', 'request_domain': 'ru', 'id': 516000,
                   'date1': date_start, 'date2': date_stop, 'metrics': 'ym:s:sessions', 'dimensions': 'ym:s:date',
                   'sort': '-ym:s:date', 'offset': 1, 'limit': 61, 'accuracy': 1, 'proposedAccuracy': 'true'}
    response_ses = requests.get('https://api.appmetrica.yandex.ru/stat/v1/data', params=params_sess,
                                headers={'Authorization': 'OAuth AAAAA'}).json()
    L = [[i['dimensions'][0]['name'], int(i['metrics'][0])] for i in response_ses['data']]
    L.sort()
    return L


def main():
    analytics = initialize_analyticsreporting()
    historyDF = pd.read_csv(filePath, delimiter=';')
    historyDF.drop(historyDF.tail(6).index, inplace=True)  # удаляем пердпоследнюю неделю

    GA = get_report(analytics, date_start, date_stop)
    GA_df = pd.DataFrame.from_records(GA, columns=['date', 'sessionGaD'])
    GA_df['date'] = GA_df['date'].str[:4] + "-" + GA_df['date'].str[4:6] + "-" + GA_df['date'].str[-2:]
    GA_df['sessionGaD'] = GA_df['sessionGaD'].apply(np.int64)

    app = dataFromAppMetrika()
    App_df = pd.DataFrame.from_records(app, columns=['date', 'sessionAppD'])
    mergeData = pd.merge(GA_df, App_df)

    FinalDF = pd.concat([historyDF, mergeData], sort=False)  # полный dataFrame с обновленной последеней неделей
    FinalDF.to_csv(filePath, index=False, sep=';')


if __name__ == '__main__':
    main()