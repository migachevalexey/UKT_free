import json
import requests
import facebook
import pprint

my_app_id = '1737540000000'
my_app_secret = 'xxx' # fb_ukt_secret in connections_key.json

with open('C:/Python/ukt/pass/ukt_facebook_token.json') as f:
    ACCESS_TOKEN = json.load(f)['ACCESS_TOKEN']
    Page_Access_Token = json.load(f)['Page_Access_Token']


graph = facebook.GraphAPI(ACCESS_TOKEN)
page_ukt_id = '200000000'

# content = graph.get_connections(page_ukt_id, 'feed')['data']
# pprint.pprint(content)
# title_id = [i['id'] for i in content]

def allPostId():
    url = f'https://graph.facebook.com/v3.2/200000/feed?limit=100&access_token={ACCESS_TOKEN}'
    title_id = []
    try:
        while True:
            content = requests.get(url).json()
            # pprint.pprint(content)
            title_id += [id['id'] for id in content['data']]
            url = requests.get(url).json()['paging']['next']
    except:
        print("List already done")
    print(len(title_id),title_id)

# allPostId()
from collections import defaultdict

# https://developers.facebook.com/docs/graph-api/reference/v3.2/insights#availmetrics
# https://developers.facebook.com/docs/platforminsights/page
period = 'day'
date_start = '2018-11-15'
date_stop = '2018-11-15'
metrics = 'page_views_total,page_impressions_organic_unique,page_impressions_paid_unique,page_impressions_viral_unique,' \
          'page_impressions_unique,page_engaged_users,page_impressions,page_views_total,page_views_logged_in_unique,page_positive_feedback_by_type'

url = f'https://graph.facebook.com/v3.2/2000000/insights/?metric={metrics}&since={date_start}&until={date_stop}&period={period}&access_token={Page_Access_Token}'
cont = requests.get(url).json()
z = {i['name']: i['values'] for i in cont['data']}
pprint.pprint(z)


s=[]
for i,j in z.items():
    for k in j:
        s.append([k['end_time'][0:10], k['value']])
# pprint.pprint(s)

d = defaultdict(list)
for row in s:
    key, val = row[0], row[1]  # или row[0], row[1]  или row['date'], row['hitid']
    d[key].append(val)

print(d)

ff=[]
for i,j in d.items():
    ff.append( [i]+j)

print(ff)

# page_impressions - Всего показов за день
# page_impressions_unique - Общий охват за день
# page_impressions_organic_unique - Органический охват за день
# page_impressions_paid_unique - Оплаченный охват
# page_impressions_viral_unique - Вирусный охват за день
# page_engaged_users - Вовлеченные пользователи Страницы за день
# page_views_total - просмотры страниц
# page_views_logged_in_unique - всего(иникальные), людей которые просмотрели
# page_positive_feedback_by_type - like, comment, link

# profile = graph.get_object(page_ukt_id) # Extracting your own profil
