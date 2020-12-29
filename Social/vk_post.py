import pprint
import requests
from urllib.parse import urlencode
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import time
import datetime
from google.cloud import bigquery
from datetime import datetime

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
# currDate = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d") # нужно для данных из GA


def vk_data_posts(token):
    params_viral = {'access_token': token, 'v': '5.85', 'owner_id': -1, 'count': 5}
    viral = requests.get('https://api.vk.com/method/wall.get', params_viral).json()
    pprint.pprint(viral['response']['items'][1:])
    postsData = [[i['id'],   datetime.utcfromtimestamp(i['date']).strftime('%Y-%m-%d'), i['copy_history'][0]['text'],
                  i['copy_history'][0]['attachments'][0]['photo']['sizes'][-1]] for i in viral['response']['items'][1:]]


'''
Дергаем статистку по каждому посту отдельности.
'''
def vk_data_postID(token):
    # a = vk_data_posts(token)
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
    print(postIdData)
    return postIdData

vk_data_postID(token)