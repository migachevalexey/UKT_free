# ВЕРСИЯ 5! POST-запросы
import json
import pprint
import requests

url_adgroups = 'https://api.direct.yandex.com/json/v5/adgroups'

with open('C:/Python/ukt/pass/ukt_Yandex_token.json') as f:
    token = 'Bearer ' + json.load(f)['token_uktads']

headers = {'Authorization': token, 'Client-Login': 'uktads',
           'Accept-Language': 'ru', "Content-Type": "application/json; charset=utf-8"}

camps_id = [35181000, 35181000, 35173000]

data_adgroups = {
    'method': 'get',
    'params': {'SelectionCriteria': {"CampaignIds": camps_id},
               "FieldNames": ['Id', 'Name']}}
response = requests.post(url_adgroups, data=json.dumps(data_adgroups), headers=headers).json()
zz = [[i['Id'], i['Name']] for i in response['result']['AdGroups']]
pprint.pprint(zz)

for i in zz:
    adgroups_update = {"method": "update",
                       "params": {"AdGroups": [{"Id": i[0], "Name": i[1].replace('(cat3|Buy): ', '')}]}}
    response = requests.post(url_adgroups, data=json.dumps(adgroups_update), headers=headers).json()
    print(response)
