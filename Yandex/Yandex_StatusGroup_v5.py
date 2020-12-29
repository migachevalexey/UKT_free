# ВЕРСИЯ 5! POST-запросы
import json
import pprint
import requests

url = 'https://api.direct.yandex.com/json/v5/campaigns'
url_adgroups = 'https://api.direct.yandex.com/json/v5/adgroups'
url_ads = 'https://api.direct.yandex.com/json/v5/ads'
url_audience = 'https://api.direct.yandex.com/json/v5/audiencetargets'

with open('C:/Python/ukt/pass/ukt_Yandex_token.json') as f:
    token = 'Bearer ' + json.load(f)['token_uktads']

headers = {'Authorization': token, 'Client-Login': 'uktads',
           'Accept-Language': 'ru', "Content-Type": "application/json; charset=utf-8"}
data_camp = {
    'method': 'get',
    'params': {'SelectionCriteria': {},
               "FieldNames": ['Id']}}
response = requests.post(url, data=json.dumps(data_camp), headers=headers).json()
camps_id = [i['Id'] for i in response['result']['Campaigns']]

data_adgroups = {
    'method': 'get',
    'params': {'SelectionCriteria': {"CampaignIds": camps_id[29:]},  # тут ПАРАМЕТРЫ, например "Ids": [],
               "FieldNames": ['CampaignId', 'Status', 'Name', "ServingStatus"]}}
response = requests.post(url_adgroups, data=json.dumps(data_adgroups), headers=headers).json()
zz = [i for i in response['result']['AdGroups'] if i['Status'] != 'ACCEPTED']
# pprint.pprint(zz)

a = []

for i in range(0, len(camps_id), 10):
    data_ads = {
        'method': 'get',
        'params': {'SelectionCriteria': {"CampaignIds": camps_id[i:i + 10]},
                   "FieldNames": ['CampaignId', 'Status', 'State', 'AdGroupId', 'StatusClarification', 'AdCategories']}}
    response = requests.post(url_ads, data=json.dumps(data_ads), headers=headers).json()
    z = [i for i in response['result']['Ads'] if i['Status'] == 'REJECTED']
    a += z
print(len(a))
pprint.pprint(a)

data_audience = {
    'method': 'get',
    'params': {'SelectionCriteria': {"CampaignIds": camps_id[:12]},
               "FieldNames": ['AdGroupId']}}
# response = requests.post(url_audience, data=json.dumps(data_audience), headers=headers).json()
