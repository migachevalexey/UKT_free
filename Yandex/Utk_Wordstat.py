import json
import time
import requests
import pandas as pd
from pandas.io import gbq
import datetime
import pprint

PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'ukt'


with open('C:/Python/ukt/pass/ukt_Yandex_token.json') as f:
    token = json.load(f)['token_mg']

url = 'https://api.direct.yandex.ru/v4/json/'
ClientLogin = 'ukt-mg'
template = {'method': 'CreateNewWordstatReport',  # 'GetWordstatReport', CreateNewWordstatReport
            'token': token,
            'Client-Login': ClientLogin}

currDate = datetime.date.today().strftime("%Y-%m-%d")

dictCategory = {

    'FishCategory': ['купить рыбу', 'купить (семга|лосось|форель)', 'купить (селедка|треска|сельдь)', 'икра купить',
                     'рыба (интернет-магазин|доставка)',
                     'купить (креветки|краб|устрицы)', 'купить (осетр|осетрина) -малек'],

    'MeatCategory': ['мясо (купить|интернет магазин)', 'купить (говядина|баранина|свинина)', 'купить стейки',
                     'купить (сало|шпик)', 'купить (колбасу|сосиски)',
                     'купить утку -чучела -охота -подсадная', 'индейка (купить|доставка)'],

    'MilkCategory': ['купить (молоко|сливки|сметана)', 'купить (йогурт|кефир|закваску)', 'купить (сыр|творог)',
                     'купить яйца -инкубатор -инкубационное -переворот -киндер -игрушка -фаберже -авито'],

    'DrinksCategory': ['купить (чай|кофе|какао) -турки',
                       'воду (купить|заказать) -счетчик -туалетную -фильтр -кулер -насос -бак -очистка -магнит -кран',
                       'вода доставка','купить сок -алоэ'],

    'VegetablesFruits': ['купить овощи', 'купить фрукты', 'купить (апельсины|мандарины|лимон) -дерево -саженцы',
                         'купить бананы  -джинсы -брюки',
                         'купить яблоки', 'купить (картошка|картофель)', 'купить (морковь|свекла) -терка -квартиру',
                         'купить (томаты|помидоры) -семена'],

    'Grocery': ['макароны купить', 'сахар купить', 'мука купить',
                'мед купить -справку -одежда -костюм -халат -водительскую -страховку -форму',
                'купить (гречка|рис)', 'купить (орехи|сухофрукты) -саженцы -дверь -квартира -стол -доска'],

    'baby': ['(подгузник|памперс|pampers) купить','(хаггис|huggies|moony|merries) купить', '(либеро|libero) купить',
             'купить детский (порошок|мыло|шампунь)','детское питание (купить|интернет магазин)',
             'купить туалетную бумагу'],
            }



def CreateReport(listPhrases):

    template.update({'method': 'CreateNewWordstatReport'}),  # 'GetWordstatReport', CreateNewWordstatReport
    template.update({"param": {"Phrases": listPhrases, 'GeoID': [1]}})
    jdata = json.dumps(template, ensure_ascii=False).encode('utf8')
    response = requests.post(url, jdata).json()
    print(response)
    return response['data']

def checkReady():
    template.update({'method': 'GetWordstatReportList'})
    jdata = json.dumps(template, ensure_ascii=False).encode('utf8')
    response = requests.post(url, jdata).json()

    return response['data'][0]['StatusReport']


def getReport(nameCat, date):

    template.update({'method': 'GetWordstatReport'})
    template.update({"param":reportID})
    jdata = json.dumps(template, ensure_ascii=False).encode('utf8')
    response = requests.post(url, jdata).json()

    return [[date, nameCat, i['SearchedWith'][0]['Phrase'], i['SearchedWith'][0]['Shows']] for i in  response['data']]


def delReport(repID):
    template.update({'method': 'DeleteWordstatReport'})
    template.update({"param": repID})
    jdata = json.dumps(template, ensure_ascii=False).encode('utf8')
    response = requests.post(url, jdata).json()


for key, val in dictCategory.items():
    reportID = CreateReport(val)
    time.sleep(60)
    z = getReport(key,currDate)
    df = pd.DataFrame.from_records(z, columns=['date', 'type', 'keyword', 'shows'])
    gbq.to_gbq(df, 'ukt.Wordstat', '78997000000', if_exists='append')
    delReport(reportID)
