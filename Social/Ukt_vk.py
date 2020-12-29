import requests
from urllib.parse import urlencode
import vk
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import time
import pandas as pd
from pandas.io import gbq
import datetime


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
SERVICE_ACCOUNT_EMAIL = 'ukt-cloud@konic-progress-196909.iam.gserviceaccount.com'
DISCOVERY_URI = 'https://analyticsreporting.googleapis.com/$discovery/rest'
KEY_FILE_LOCATION = r'C:\Python\ukt\pass\uktOwox-d00000000e12.p12'
VIEW_ID = '115850000'  # id нужного представления в GA

with open(r'C:\Python\ukt\pass\vkToken.txt') as f:
    token = f.readline()

session = vk.Session(access_token=token)
vk_api = vk.API(session, v='5.92')
adsAccount = 1604800000


currDate = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


def vk_data(token, sdate):
    params = {'access_token': token, 'v': '5.85', 'clien_id': 100, 'group_id': 1, 'date_from': sdate,
              'date_to': sdate}
    r_profile = requests.get('https://api.vk.com/method/stats.get', params).json()
    r_members = requests.get('https://api.vk.com/method/groups.getMembers', params).json()

    # f"Просмотры - НЕ НАДО!: {r_profile['response'][0]['visitors']['views']}, уникальные посетители - НЕ НАДО!: {r_profile['response'][0]['visitors']['visitors']}", '\n',
    # f"Полный охват: {r_profile['response'][0]['reach']['reach']}, Охват подписчиков: {r_profile['response'][0]['reach']['reach_subscribers']}",'\n',
    # f"Комментарии: {r_profile['response'][0]['activity']['comments']}, Скрыли из новостей - НЕ НАДО: {r_profile['response'][0]['activity']['hidden']}, "
    # f"Нравится: {r_profile['response'][0]['activity']['likes']}, Подписки: {r_profile['response'][0]['activity']['subscribed']}, Отписки: {r_profile['response'][0]['activity']['unsubscribed']}")

    vk = [sdate] + [
        r_profile['response'][0]['reach'].get('reach', 0),
        r_profile['response'][0]['reach'].get('reach_subscribers', 0),
        r_profile['response'][0]['activity'].get('comments', 0),
        r_profile['response'][0]['activity'].get('likes', 0),
        r_profile['response'][0]['activity'].get('subscribed', 0),
        r_profile['response'][0]['activity'].get('unsubscribed', 0),
        r_profile['response'][0]['activity'].get('copies', 0)]+[r_members['response']['count']]

    return vk


def ga_data(analytics, sdate):
    # help -> https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet?hl=ru
    body = {
        'reportRequests': [
            {'viewId': VIEW_ID,
             'dateRanges': [{'startDate': f'{sdate}', 'endDate': f'{sdate}'}],
             "samplingLevel": 'LARGE',
             'metrics': [{"expression": "ga:sessions"}, {"expression": "ga:bounceRate"},
                         {"expression": "ga:pageviewsPerSession"}, {"expression": "ga:avgSessionDuration"},
                         {"expression": "ga:transactions"}, {"expression": "ga:transactionRevenue"}],
             "dimensionFilterClauses": [{
                 "filters": [
                     {"dimensionName": "ga:source",
                      "expressions": ["vk"],
                      "operator": "PARTIAL"
                      }
                 ]}], }]}

    request = analytics.reports().batchGet(body=body).execute()  # запрос API
    API_data = request['reports'][0]['data']['rows'][0]['metrics'][0]['values'] + ['Vkontakte']
    API_data[0] = int(API_data[0])
    API_data[1] = round(float(API_data[1]), 2)
    API_data[2] = round(float(API_data[2]), 2)
    API_data[3] = time.strftime("%M:%S", time.gmtime((int(float(API_data[3])))))
    API_data[4] = int(API_data[4])
    API_data[5] = float(API_data[5])

    return API_data


def fin_obrabotchik(analytics, token, sdate):

    finalData = vk_data(token, sdate)[:-1] + ga_data(analytics, sdate)+[vk_data(token, sdate)[-1]]

    return finalData


def dataToGBQ(analytics, token, sdate):
    z = [fin_obrabotchik(analytics, token, sdate)]

    labels = ['date', 'reach', 'reach_subscribers', 'comments',
              'likes', 'subscribed', 'unsubscribed', 'copies', 'sessions', 'bounceRate', 'pageviews', 'duration',
              'transactions', 'revenue', 'socialNet', 'members']  # порядок столбцов

    df = pd.DataFrame.from_records(z, columns=labels)
    gbq.to_gbq(df, 'Temp.Social', '78997000000', if_exists='append')


def initialize_analyticsreporting():
    credentials = ServiceAccountCredentials.from_p12_keyfile(
        SERVICE_ACCOUNT_EMAIL, KEY_FILE_LOCATION, scopes=SCOPES)
    http = credentials.authorize(httplib2.Http())
    analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)

    return analytics


def apiData():
    curDate = datetime.date.today() - datetime.timedelta(days=1)
    adGroups = vk_api.ads.getAds(account_id=adsAccount)
    # получаем все активные кампании
    AdIdName = [[int(i['id']), i['campaign_id'], i['name']] for i in adGroups if i['status'] == 1]
    # получаем все активные группы
    adGroupsId = ','.join(map(str, [i['id'] for i in adGroups if i['status'] == 1]))
    cost = vk_api.ads.getPostsReach(account_id=adsAccount, ids_type='ad', ids=adGroupsId, period='day',
                                    date_from=curDate, date_to=curDate)
    ad_reach = [[currDate, i['id'], i['reach_total'], i['reach_subscribers'], i['links']] for i in cost]
    # задержка иначе vk ругается на слишком частое колво api запросов
    time.sleep(3)
    campaigns = vk_api.ads.getCampaigns(account_id=adsAccount)
    campaignsIdName = [[i['id'], i['name']] for i in campaigns if i['status'] == 1]
    adName = pd.DataFrame.from_records(AdIdName, columns=['adid', 'campid', 'adname'])
    campName = pd.DataFrame.from_records(campaignsIdName, columns=['campid', 'campname'])
    adReach = pd.DataFrame.from_records(ad_reach, columns=['date', 'adid', 'reach_total', 'reach_subscribers', 'links'])
    adName = pd.merge(adName, campName, on=['campid'])
    adReach = pd.merge(adReach, adName, on=['adid'])
    adReach = adReach[
        ['date', 'campid', 'campname', 'adid', 'adname',
         'reach_total', 'reach_subscribers', 'links']]

    gbq.to_gbq(adReach, 'Temp.SocialAds', '78997000000', if_exists='append')


def main():
    analytics = initialize_analyticsreporting()
    dataToGBQ(analytics, token, currDate)
    time.sleep(5)
    apiData()


if __name__ == '__main__':
    main()
