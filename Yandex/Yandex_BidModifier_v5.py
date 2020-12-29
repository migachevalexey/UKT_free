# ВЕРСИЯ 5! POST-запросы
import json
import pprint
import requests

url = 'https://api.direct.yandex.com/json/v5/campaigns'
url_adgroups = 'https://api.direct.yandex.com/json/v5/adgroups'
url_bid = 'https://api.direct.yandex.com/json/v5/bidmodifiers'

with open('C:/Python/ukt/pass/ukt_Yandex_token.json') as f:
    token = 'Bearer ' + json.load(f)['token']

headers = {'Authorization': token, 'Client-Login': 'context.ukt',
           'Accept-Language': 'ru', "Content-Type": "application/json; charset=utf-8"}
data_camp = {
    'method': 'get',
    'params': {'SelectionCriteria': {"States": ["ON", "SUSPENDED"]},
               "FieldNames": ['Id']}}
response = requests.post(url, data=json.dumps(data_camp), headers=headers).json()
camps_id = [i['Id'] for i in response['result']['Campaigns']]

data_adgroups = {
    'method': 'get',
    'params': {'SelectionCriteria': {"CampaignIds": camps_id[:9], "Statuses": ["ACCEPTED"]},
               "FieldNames": ['Id']}}
response = requests.post(url_adgroups, data=json.dumps(data_adgroups), headers=headers).json()
adgroups_id = [i['Id'] for i in response['result']['AdGroups']]

data = {
    'method': 'get',
    'params': {'SelectionCriteria': {"CampaignIds": camps_id,
                                     "Types": ["MOBILE_ADJUSTMENT", "DEMOGRAPHICS_ADJUSTMENT", "RETARGETING_ADJUSTMENT",
                                               "REGIONAL_ADJUSTMENT", "VIDEO_ADJUSTMENT"],
                                     "Levels": ["CAMPAIGN"]},
               "FieldNames": ["CampaignId"], "MobileAdjustmentFieldNames": ["BidModifier"],
               "RetargetingAdjustmentFieldNames": ["Enabled", "BidModifier"],
               "DemographicsAdjustmentFieldNames": ["BidModifier"], "RegionalAdjustmentFieldNames": ["BidModifier"]}

}

response = requests.post(url_bid, data=json.dumps(data), headers=headers).json()
pprint.pprint(response['result']['BidModifiers'])
