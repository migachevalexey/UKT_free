import re
from builtins import sorted
from pprint import pprint
import pandas as pd
from pandas.io import gbq

projectid = '78997000000'  # http://prntscr.com/k0dus1
PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'Temp'
TABLE = 'YandexKeyword'
import_action = 'replace'  # append , 'replace', 'fail'
from urllib.parse import urlencode

import requests
import json

# получение токена, формируем ссылку, печатаем ее и переходим по ней вживую
# щелкаем - "Разрешить" и нам выдают токен!
# from future.backports.urllib import response
# AUTHORIZE_URL = 'https://oauth.yandex.ru/authorize'
# APP_ID = 'b000000'
# auth_data = {'response_type': 'token',
#         'client_id': APP_ID }
# print('?'.join((AUTHORIZE_URL, urlencode(auth_data))))


pathPass = r'C:\Python\ukt\pass\MySQL_db_connect.json'

with open(pathPass) as f:
    param = json.load(f)

"""
Поисковые фразы(keywords) Яндекс Organic по которым были заказы  
https://tech.yandex.ru/metrika/doc/api2/api_v1/attrandmetr/dim_all-docpage/
https://tech.yandex.ru/metrika/doc/api2/api_v1/data-docpage/
"""

TOKEN = param['yandexMetrikaukt']
STAT_URL = 'https://api-metrika.yandex.ru/stat/v1/data'
METRIC_ID = 942000
date_start = '2018-03-01'
date_stop = '2018-12-31'

headers = {
    'Authorization': 'OAuth {}'.format(TOKEN),
    'Content-Type': 'application/x-yametrika+json',
    'User-Agent': 'Chrome'
}
params = {
    'ids': METRIC_ID,
    'date1': date_start,
    'date2': date_stop,
    'accuracy': 'full',
    'metrics': 'ym:s:ecommercePurchases,ym:s:ecommerceRevenue',
    'dimensions': 'ym:s:purchaseID,ym:s:lastSearchPhrase',
    'group': 'day',
    'filters': "ym:s:SearchEngineRootName=='Яндекс'",
    'limit': 100000
}
response = requests.get(STAT_URL, params, headers=headers).json()

api_data = {}
q =[[i['dimensions'][0]['name'], i['dimensions'][1]['name']] for i in response['data'] if i['dimensions'][1]['name'] is not None ]
for i in response['data']:
    # if i['dimensions'][1]['name'] != None:
    api_data[int(i['dimensions'][0]['name'])] = [i['dimensions'][1]['name'], i['metrics'][1]]


keywordTransactions = {}
for j in api_data.values():
    # print(j)
    keywordTransactions[j[0]] = keywordTransactions.get(j[0], 0) + 1

print(f'Итого транзакций - {int(response["totals"][0])}шт. на сумму {int(response["totals"][1])}руб')
z = sorted(keywordTransactions.items(), key=lambda x: x[1], reverse=True)
pprint(z)
print(date_start, date_stop)
df = pd.DataFrame.from_records(q, columns=['transaction','keyword'])
gbq.to_gbq(df, f'{DATASET_ID}.{TABLE}', projectid, if_exists=import_action)  # отправка данных в GBQ
