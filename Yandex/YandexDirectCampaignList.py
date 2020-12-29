# ВЕРСИЯ 5! POST-запросы
# https://tech.yandex.ru/direct/doc/ref-v5/campaigns/get-docpage/

import json
import pprint
import requests
import csv

'''
Получаем список из ID кампаний
Если тянем старые данные, не забываем добавить архивированные кампании - ARCHIVED

Функция region_data() тянет из справичника названия регионов, 
она нужна для того что бы потом матчить id с name региона и группировать города по регионам - например Свердловская область
'''

url = 'https://api.direct.yandex.com/json/v5/campaigns'
url_region = 'https://api.direct.yandex.com/json/v5/dictionaries'

with open('../pass/yandexTokenmg.txt') as f:
    token = 'Bearer ' + f.readline()
print(token)

ClientLogin = 'ukt.mg'

headers = {'Authorization': token, 'Client-Login': ClientLogin,
           'Accept-Language': 'ru', "Content-Type": "application/json; charset=utf-8"}

data = {
    'method': 'get',  # Здесь УКАЗЫВАЕМ метод
    'params': {'SelectionCriteria': {"States": ["CONVERTED","ENDED","ON","SUSPENDED"], "Types":['YandexDirectCampaignList.py']}, # если тянем старые данные то еще добавить "ARCHIVED"

               "FieldNames": ['Id', 'Name', 'Statistics', 'State', 'Status']}
}

response = requests.post(url, data=json.dumps(data), headers=headers).json()
print(response)
campaignData = response['result']['Campaigns']


def campaignIDs():
    listID = [i['Id'] for i in campaignData]
    return listID


def campaignID_Name():
    listIDName = [[i['Id'],i['Name']] for i in campaignData]
    with open("id_name_campaignYandex.csv", "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerows(listIDName[1:])
campaignID_Name()
