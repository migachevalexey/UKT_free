"""Analytics Reporting API V4."""
import csv
import pprint
import time

import numpy as np
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
VIEW_ID = '115850000'  # id

date_start = '2019-09-02'
date_stop = '2019-09-08'

pd.set_option('display.max_rows', 1000) # для вывода на  печать полного dataFrame
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 300)
pd.options.display.float_format = "{:,.2f}".format # полные цифры, без e

def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_p12_keyfile(
        SERVICE_ACCOUNT_EMAIL, KEY_FILE_LOCATION, scopes=SCOPES)
    http = credentials.authorize(httplib2.Http())
    analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)

    return analytics


def get_report(analytics, date_start,date_stop):
    q='37277$|^37265$|^37257$|^37261$|^37601$|^37609$|^37273$|^37597$|^37605$|^37589$|^37593'
    QUERY = {
        'reportRequests': [ # https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet?hl=ru#DimensionFilter
            {'viewId': VIEW_ID,
             'dateRanges': [{'startDate': date_start, 'endDate': date_stop}],
             "samplingLevel": 'LARGE',
             "dimensions": [
                 {"name": "ga:yearMonth"},{"name": "ga:searchUsed"},{"name": "ga:channelGrouping"},{"name": "ga:deviceCategory"}],
             'metrics': [{"expression": "ga:sessions"},{"expression": "ga:transactions"}, {"expression": "ga:transactionRevenue"}],
             # "dimensionFilterClauses": [{
             #     "operator": 'AND',
             #     "filters": [
             #         {"dimensionName": "ga:internalPromotionId",
             #          "expressions": [q],
             #          "operator": "REGEXP"},
             #         # {"dimensionName": "ga:productListName",
             #         #  "expressions": ['^action'],
             #         #  "operator": "REGEXP"}
             #     ]}],
             'pageSize': 10000}
        ]}

    request = analytics.reports().batchGet(body=QUERY).execute()  # запрос API

    rowCount = request['reports'][0]['data'].get('rowCount',0)
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


def main():
    pd.options.display.float_format = "{:,.2f}".format  # полные цифры, без e
    base = datetime.datetime.today()
    date_list = [(base - datetime.timedelta(days=x)) for x in range(1,617,7)]
    date_list2 = [(i - datetime.timedelta(days=6)).strftime('%Y-%m-%d') for i in date_list]  # .strftime('%Y-%m-%d')
    date_list = [x.strftime('%Y-%m-%d') for x in date_list]
    finaldatelist = list(zip(date_list2,date_list))
    analytics = initialize_analyticsreporting()
    emptyData =pd.DataFrame()
    for j in finaldatelist[:4]:
        print(j)
        z=get_report(analytics, j[0], j[1])
        F_df = pd.DataFrame.from_records(z, columns=['yearMonth','searchUsed','sourceMedium','siteType', 'sessions','transactions','transactionRevenue'])
        emptyData=pd.concat([emptyData,F_df])
        # time.sleep(3)

    emptyData['transactionRevenue']= emptyData['transactionRevenue'].astype(float).astype(int)  #.apply(np.int64)
    a=emptyData.pivot_table(index=['yearMonth','siteType','searchUsed','sourceMedium'],  values=['sessions','transactions','transactionRevenue'],
                            aggfunc={'sessions':np.sum, 'transactions': np.sum, 'transactionRevenue':np.sum}).reset_index()

    print(a)
    # F_df['view'] = F_df['view'].apply(np.int64)
    # print(F_df.head())
    # gbq.to_gbq(F_df, 'Mig_Data.Trafic', '78997000000', if_exists='append')
    # F_df['ctr'] =F_df['clicks']/F_df['view']
    a.to_excel(f'C:/Python/ukt/txtcsvFile/Morozovfinal_{date_start}.xlsx', index=False)


if __name__ == '__main__':
    main()