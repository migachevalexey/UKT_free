# ВЕРCИЯ 4.
import json
import requests
import pprint


with open('C:/Python/pass/Yandex_token_kor.json') as f:
    data = json.load(f)
token = data['token']
url ='https://api.direct.yandex.ru/v4/json/'
ClientLogin = 'kor.medianation'
data = {'method':   'CreateNewWordstatReport', #'GetWordstatReport',
    'token': token,
    'Client-Login': ClientLogin,
    # "param": 876851698,
      "Phrases": ["кукла купить лола оригинал", "кукла сюрприз шар","кукла лола цена"] }


jdata = json.dumps(data, ensure_ascii=False).encode('utf8')
response = requests.post(url,jdata).json()
print(response)
# for i in response['data']:
#     pprint.pprint(i['SearchedWith'][0])


