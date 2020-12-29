import re
import requests
from urllib.parse import urlencode
from datetime import datetime
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import time
import pandas as pd
from pandas.io import gbq
import datetime
from google.cloud import bigquery

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
SERVICE_ACCOUNT_EMAIL = 'ukt-cloud@konic-progress-196909.iam.gserviceaccount.com'
DISCOVERY_URI = 'https://analyticsreporting.googleapis.com/$discovery/rest'
KEY_FILE_LOCATION = r'C:\Python\ukt\pass\uktOwox-d00000000e12.p12'
VIEW_ID = '115850000'  # id нужного представления в GA
PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'Temp'
TABLE_ID = 'VKPosts'
TABLE_ID_Temp = 'VKPosts_Temp'
client = bigquery.Client(project=PROJECT_ID)


with open(r'C:\Python\ukt\pass\vkToken.txt') as f:
    token = f.readline()
currDate = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")  # нужно для данных из GA

'''
Выдергиваем базовую информацию о последних 100 постах.
URL есть не во всех постах, дергаем там где он есть - сделано через Ж..У, но работает 
И потом мержим 2 dataFrame слева(как left join) - что бы там где нет URL не обрезалась строка
Функция возвращает список списков, т.к. нам потом еще с этим списком работать
'''


def vk_data_posts(token):
    params_viral = {'access_token': token, 'v': '5.85', 'owner_id': -1, 'count': 100}
    viral = requests.get('https://api.vk.com/method/wall.get', params_viral).json()
    postsData = [[i['id'], i['likes']['count'], i['reposts']['count'], i['comments']['count'],
                  ' '.join(re.findall('\w+', i['text'][0:100])),
                  datetime.datetime.fromtimestamp(i['date']).strftime('%Y-%m-%d'),
                  f"https://vk.com/vipukt?w=wall-1_{i['id']}"] for i in viral['response']['items']]
    urlData = []
    for i in range(100):
        try:
            urlData.append([viral['response']['items'][i]['id'],
                            viral['response']['items'][i]['attachments'][1].get('link', {}).get('url', '-').strip(
                                '\n').replace(' ', '')])
        except:
            continue
    # z = [[i['id'], re.search(r'\bhttps://vk\.cc/\w*\b', i['text']).group(0)] for i in viral['response']['items'] if re.search( r'\bhttps://vk\.cc/\w*\b',i['text']) is not None ]

    F_df = pd.DataFrame.from_records(postsData)
    S_df = pd.DataFrame.from_records(urlData)
    mergedData = pd.merge(F_df, S_df, how='left', on=0)

    return mergedData.values.tolist()


'''
Дергаем статистку по каждому посту отдельности.
'''

def vk_data_postID(token):
    postIdData = []
    for i in vk_data_posts(token):
        params_viral_stat = {'access_token': token, 'v': '5.87', 'owner_id': -1, 'post_id': i[0]}
        viral_stat = requests.get('https://api.vk.com/method/stats.getPostReach', params_viral_stat).json()
        time.sleep(1.5)
        postIdData.append(
            [i[0], viral_stat['response'][0]['reach_total'], viral_stat['response'][0]['reach_subscribers'],
             viral_stat['response'][0]['links'], 'Vkontakte'])

    # Охват - viral_stat['response'][0]['reach_total']/ viral_stat['response'][0]['reach_subscribers']
    # Переходы по ссылке - viral_stat['response'][0]['links']

    return postIdData


# Запрос в GA на получение определенных данных за определенные даты с фильтрами на источник\канал\кампанию\группу объявлений

def GA_Body(analytics, sdate, edate, camp, cont):
    # help -> https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet?hl=ru
    body = {
        'reportRequests': [
            {'viewId': VIEW_ID,
             'dateRanges': [{'startDate': f'{sdate}', 'endDate': f'{edate}'}],
             "samplingLevel": 'LARGE',
             'metrics': [{"expression": "ga:sessions"}, {"expression": "ga:bounceRate"},
                         {"expression": "ga:pageviewsPerSession"}, {"expression": "ga:avgSessionDuration"},
                         {"expression": "ga:transactions"}, {"expression": "ga:transactionRevenue"}],
             "dimensionFilterClauses": [{
                 "operator": 'AND',
                 "filters": [
                     {"dimensionName": "ga:source",
                      "expressions": ["vk"],
                      "operator": "EXACT"
                      },
                     {"dimensionName": "ga:medium",
                      "expressions": ["social"],
                      "operator": "EXACT"
                      },
                     {"dimensionName": "ga:campaign",
                      "expressions": [f"{camp}"],
                      "operator": "EXACT"
                      },
                     {"dimensionName": "ga:adContent",
                      "expressions": [f"{cont}"],
                      "operator": "EXACT"
                      }
                 ]}], }]}

    request = analytics.reports().batchGet(body=body).execute()  # запрос API
    API_data = request['reports'][0]['data']['rows'][0]['metrics'][0]['values']
    # перенести на уровень обработки dataFrame
    API_data[0] = int(API_data[0])
    API_data[1] = round(float(API_data[1]), 2)
    API_data[2] = round(float(API_data[2]), 2)
    API_data[3] = time.strftime("%M:%S", time.gmtime((int(float(API_data[3])))))
    API_data[4] = int(API_data[4])
    API_data[5] = float(API_data[5])

    return API_data


# Вытягивае из строки utm_content и utm_campaign. На выходе имеем список
def stringPart(s):
    if s.find('utm_content') != -1:
        a = re.findall('utm_campaign.*', s)
        return a[0].split('&')
    else:
        exit


# получаем все посты у которых есть ссылка с UTM разметкой и по этой разметке вытягиваем из GA данные
def GA_Data(analytics, token):
    d = []
    for i in [[i[0]] + stringPart(str(i[-1])) for i in vk_data_posts(token) if stringPart(str(i[-1])) is not None]:
        try:
            if len(i) >= 3:
                s = i[2].replace('utm_content=', '')[0:8]
                d.append(
                    [i[0]] + GA_Body(analytics, f'{s[-4:]}-{s[2:4]}-{s[:2]}', currDate,
                                     i[1].replace('utm_campaign=', ''), i[2].replace('utm_content=', '')))
        except:
            continue

    return d


# мержим данные. Вначале мержим данные по постам потом слева подставляем данные из GA(те, что удалось вытянуть)
# и кидаем все это добро во временную таблицу VKPosts_Temp
def fin_obrabotchik(analytics, token):
    primary_data_VK = vk_data_posts(token)
    secondary_data_VK = vk_data_postID(token)
    third_data_GA = GA_Data(analytics, token)

    F_labels = ['Id', 'likes', 'reposts', 'comments', 'name', 'date', 'postUrl', 'url']
    S_labels = ['Id', 'reach_total', 'reach_subscribers', 'links', 'socialNet']
    Th_labels = ['Id', 'sessions', 'bounceRate', 'pagePerSession', 'duration', 'transactions', 'revenue']
    F_df = pd.DataFrame.from_records(primary_data_VK, columns=F_labels)
    S_df = pd.DataFrame.from_records(secondary_data_VK, columns=S_labels)
    Th_df = pd.DataFrame.from_records(third_data_GA, columns=Th_labels)

    mergedData = pd.merge(F_df, S_df, on=['Id'])
    mergedData_Final = pd.merge(mergedData, Th_df, how='left', on=['Id'])
    mergedData_Final = mergedData_Final[
        ['Id', 'date', 'name', 'reach_total', 'reach_subscribers', 'links', 'likes', 'reposts', 'comments', 'url',
         'sessions', 'bounceRate', 'pagePerSession', 'duration', 'transactions', 'revenue', 'postUrl',
         'socialNet']]  # определяем порядок столбцов
    # mergedData_Final.to_csv('111.csv')

    gbq.to_gbq(mergedData_Final, f'{DATASET_ID}.{TABLE_ID_Temp}', '78997000000', if_exists='replace')


# В постоянную таблицу подтягиваем данные из временной. Существующие данные UPDATE(т.к. статистика со временем изменятся), недостающие - INSERT.
def BQ_Merge_data():
    SQL_merge = f'MERGE {DATASET_ID}.{TABLE_ID} D ' \
                f'USING {DATASET_ID}.{TABLE_ID_Temp} S ' \
                f'ON D.id = S.id ' \
                f'WHEN MATCHED THEN ' \
                f'UPDATE SET reach_total=s.reach_total, reach_subscribers=s.reach_subscribers, links=s.links, likes=s.likes, reposts=s.reposts, comments=s.comments, sessions=s.sessions, ' \
                f'bounceRate=s.bounceRate, pagePerSession=s.pagePerSession, duration=s.duration, transactions=s.transactions, revenue=s.revenue ' \
                f'WHEN NOT MATCHED THEN ' \
                f'INSERT (Id,date, name, reach_total, reach_subscribers, links, likes, reposts, comments, url, sessions, bounceRate, pagePerSession, duration, transactions, revenue,postUrl, socialNet) ' \
                f'VALUES(Id,date, name, reach_total, reach_subscribers, links, likes, reposts, comments, url, sessions, bounceRate, pagePerSession, duration, transactions, revenue,postUrl, socialNet)'

    query_job = client.query(SQL_merge)
    z = query_job.result()
    print(query_job.state, '\n', query_job.num_dml_affected_rows, "rows affected")
    return z


def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_p12_keyfile(
        SERVICE_ACCOUNT_EMAIL, KEY_FILE_LOCATION, scopes=SCOPES)
    http = credentials.authorize(httplib2.Http())
    analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)

    return analytics


def main():
    analytics = initialize_analyticsreporting()
    fin_obrabotchik(analytics, token)
    BQ_Merge_data()


if __name__ == '__main__':
    main()
