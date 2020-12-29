import datetime
import urllib3
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
import httplib2
import pandas as pd
from pandas.io import gbq
import MySQLdb
import json
urllib3.disable_warnings()

"""
    Тянем из Orders заказы, у которых Medium = null Смотрим их SID в siteDB ukt_session_visit_report 
    Потом смотрим источник\Канал и прочие параметры по этому SID в GA(2 запроса, т.к. больше 9 параметров надо вытянуть), удаляем дубли
    мержим между собой эти dataFrame и обновляем  источник\Канал и пр. в Orders 
    Результаты пишем в лог-файл. Временную таблицу SID вконце удаляем
"""

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
SERVICE_ACCOUNT_EMAIL = 'ukt-cloud@konic-progress-196909.iam.gserviceaccount.com'
DISCOVERY_URI = 'https://analyticsreporting.googleapis.com/$discovery/rest'
KEY_FILE_LOCATION = r'C:\Python\ukt\pass\uktOwox-d00000000e12.p12'
PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'Mig_Data'
file_db_connect = 'C:/Python/ukt/pass/MySQL_db_connect.json'
VIEW_ID = '115850000'  # id представления Raw Data (GTM) в GA-8
client = bigquery.Client(project=PROJECT_ID)
dataset = client.dataset('Mig_Data')

DateStart = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
DateStop = DateStart


def transactionId():
    QUERY = "SELECT  transaction FROM `{}.Mig_Data.Orders` where  regexp_contains( orderSource ,'Web') and date between '{}' and '{}' " \
            "and Medium is null".format(PROJECT_ID, DateStart, DateStop)
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return [row.transaction for row in rows]


def sql_db_select():
    listSID=transactionId()
    with open(file_db_connect) as f:
        param_сonnect = json.load(f)
    db_connect = MySQLdb.connect(user=param_сonnect['user'], passwd=param_сonnect['passwd'],
                                 host=param_сonnect['host'], db=param_сonnect['db_sess'], charset='cp1251')
    cursor = db_connect.cursor()
    sql_query = f'select session_id, master_order_id from ukt_session_visit_report WHERE master_order_id in ({",".join(listSID)})'
    cursor.execute(sql_query)
    sql_data = cursor.fetchall()  # r=cursor.fetchmany(5)
    cursor.close()
    db_connect.close()

    return list(sql_data)


def queryData(QUERY):
    query_job = client.query(QUERY)
    rows = query_job.result()
    if QUERY.find('UPDATE') != -1:
        print(query_job.state, query_job.num_dml_affected_rows, "rows affected")
        with open(r'c:\python\ukt\Cron\log\OrdersFullToBQ.txt', 'a') as f:
            f.write(f'На дату: {DateStop} ' + f' обновлен source\medium через SID {str(query_job.num_dml_affected_rows)} записей в таблице Mig_Data.Orders' + '\n')
    return rows


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

    API_data = [list(i['dimensions']) for i in API_data]

    return API_data


def fin_obrabotchik(analytics, dstart, dstop):
    sqlData = sql_db_select()
    F_body = {
        'reportRequests': [
            {'viewId': VIEW_ID,
             'dateRanges': [{'startDate': f'{dstart}', 'endDate': f'{dstop}'}],
             "samplingLevel": 'LARGE',
             "dimensions": [{"name": "ga:dimension3"},
                            {"name": "ga:browser"},
                            {"name": "ga:userType"},
                            {'name': 'ga:channelGrouping'},
                            {"name": "ga:medium"}, {"name": "ga:source"},
                            {"name": "ga:campaign"}, {'name': 'ga:keyword'},
                            {"name": "ga:deviceCategory"}],
             'metrics': [{"expression": "ga:sessions"}, ],
             "dimensionFilterClauses": [{
                 "operator": 'AND',
                 "filters": [
                     {"dimensionName": "ga:dimension3",
                      "expressions": ['|'.join([i[0] for i in sqlData])],
                      "operator": "REGEXP"},]}],
             'pageSize': 10000}
        ]}

    S_body = {
        'reportRequests': [
            {'viewId': VIEW_ID,
             'dateRanges': [{'startDate': f'{dstart}', 'endDate': f'{dstop}'}],
             "samplingLevel": 'LARGE',
             "dimensions": [{"name": "ga:dimension3"}, {"name": "ga:city"},
                            {"name": "ga:dimension6"}],
             'metrics': [{"expression": "ga:sessions"}],
             "dimensionFilterClauses": [{
                 "operator": 'AND',
                 "filters": [
                     {"dimensionName": "ga:dimension3",
                      "expressions": ['|'.join([i[0] for i in sqlData])],
                      "operator": "REGEXP"},]}],
             'pageSize': 10000}]}

    primary_data = get_report(analytics, F_body)
    secondary_data = get_report(analytics, S_body)

    F_df = pd.DataFrame.from_records(primary_data, columns=['SID','browser','userTypeGA','channel','medium','source','campaign','keyword','device'])
    S_df = pd.DataFrame.from_records(secondary_data, columns=['SID','city','clientId'])
    sql_df = pd.DataFrame.from_records(sqlData, columns=['SID', 'transaction'])
    print(f'F_df - {len(F_df)} ', f'S_df - {len(S_df)}')

    finalData = pd.merge(F_df, S_df, how='left', on=['SID'])  # делаем join слева меньшего к большему
    finalData = pd.merge(finalData, sql_df, how='left', on=['SID'])

    finalData = finalData[['transaction', 'channel', 'medium', 'source', 'campaign', 'keyword',
                            'device', 'browser', 'clientId', 'city', 'userTypeGA']]  # определяем порядок и колво нужных столбцов

    print(f'Final_df - {len(finalData)}')
    finalData.drop_duplicates(subset='transaction', inplace=True)
    print(f'Final_df one duplicates - {len(finalData)}')
    gbq.to_gbq(finalData, 'Mig_Data.SID', '78997000000', if_exists='replace')


def main():
    analytics = initialize_analyticsreporting()
    fin_obrabotchik(analytics, DateStart, DateStop)

    QUERY_upd = f"UPDATE `Mig_Data.Orders` o SET o.channel=t.channel, o.source=t.source, o.medium=t.medium, o.campaign=t.campaign, o.keyword=t.keyword," \
                f"o.clientId=t.clientId, o.userTypeGA=t.userTypeGA, o.browser=t.browser, o.device=t.device, o.city=t.city " \
                f"FROM `Mig_Data.SID` t " \
                f"WHERE o.transaction=t.transaction"
    queryData(QUERY_upd)
    table_ref = client.dataset(DATASET_ID).table('SID')
    if client.get_table(table_ref):
        client.delete_table(table_ref)


if __name__ == '__main__':
    main()