"""Analytics Reporting API V4."""
import csv
import pprint
from collections import defaultdict
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import pandas as pd

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
SERVICE_ACCOUNT_EMAIL = 'ukt-cloud@konic-progress-196909.iam.gserviceaccount.com'
DISCOVERY_URI = 'https://analyticsreporting.googleapis.com/$discovery/rest'
KEY_FILE_LOCATION = r'C:\Python\ukt\pass\uktOwox-d00000000e12.p12'
VIEW_ID = '115850000'


df = pd.read_csv('./txtcsvFile/17промо.csv', delimiter=';')

for index, row in df.iterrows():
    print(row['start'], row['finish'], row['promoaction_id'])

# df_IdDateAction = df[['promoaction_id','start','finish']].drop_duplicates().reset_index().drop('index',axis=1)
# df_IdDateAction['start'] = df_IdDateAction['start'].map(lambda x: str(x)[:10])
# df_IdDateAction['finish'] = df_IdDateAction['finish'].map(lambda x: str(x)[:10])


def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_p12_keyfile(
        SERVICE_ACCOUNT_EMAIL, KEY_FILE_LOCATION, scopes=SCOPES)
    http = credentials.authorize(httplib2.Http())
    analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)

    return analytics


def get_report(analytics, date_start,date_stop, idAction):
    QUERY = {
        'reportRequests': [
            {'viewId': VIEW_ID,
             'dateRanges': [{'startDate': date_start, 'endDate': date_stop}],
             "samplingLevel": 'LARGE',
             "dimensions": [
                 {"name": "ga:productSku"}],
             'metrics': [{"expression": "ga:uniquePurchases"},{"expression": "ga:itemRevenue"}, {"expression": "ga:itemQuantity"}, {"expression": "ga:revenuePerItem"},{"expression": "ga:quantityCheckedOut"}],
             "dimensionFilterClauses": [{
                 "operator": 'AND',
                 "filters": [
                     {"dimensionName": "ga:productListName",
                      "expressions": [idAction],
                      "operator": "REGEXP"}
                 ]}],
             'pageSize': 10000}
        ]}

    request = analytics.reports().batchGet(body=QUERY).execute()  # запрос API
    rowCount = request['reports'][0]['data'].get('rowCount',0)

    if rowCount > 0:
        API_data = request['reports'][0]['data']['rows']
        API_data = [list(i['dimensions']) + list(i['metrics'][0]['values']) for i in API_data]
        return API_data
    else:
        return []

def main():
    finalData = []
    analytics = initialize_analyticsreporting()
    for index, row in df.iterrows():
        GAData = get_report(analytics, row['start'],row['finish'], str(row['promoaction_id']))
        if len(GAData) > 0:
            [i.append(row['promoaction_id']) for i in GAData]
            finalData += GAData
            GAData.clear()

    finalDF = pd.DataFrame(finalData, columns=['sku', 'заказы', 'доход', 'колво', 'цена','temp', 'id акции'])
    finalDF.drop('temp', axis=1, inplace=True)
    finalDF=finalDF.apply(pd.to_numeric)
    finalDF.to_excel('./txtcsvFile/promo37193.xlsx', index=False)

if __name__ == '__main__':
    main()