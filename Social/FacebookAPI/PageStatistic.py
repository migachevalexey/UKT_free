import requests
import facebook
import pprint
import pandas as pd
from pandas.io import gbq
import datetime
import json

my_app_id = '173000'
my_app_secret = 'xxx' # fb_ukt_secret in connections_key.json
page_ukt_id = '217000'

with open('C:/Python/ukt/pass/ukt_facebook_token.json') as f:
    # ACCESS_TOKEN =  json.load(f)['ACCESS_TOKEN']
    Page_Access_Token = json.load(f)['Page_Access_Token']

# graph = facebook.GraphAPI(ACCESS_TOKEN)

# https://developers.facebook.com/docs/graph-api/reference/v3.2/insights#availmetrics
# https://developers.facebook.com/docs/platforminsights/page
# https://developers.facebook.com/docs/graph-api/reference/v3.2/insights#--------------------

period = 'day'

# page_impressions - Всего показов за день
# page_impressions_unique - Общий охват за день
# page_impressions_organic_unique - Органический охват за день
# page_impressions_paid_unique - Оплаченный охват
# page_impressions_viral_unique - Вирусный охват за день
# page_engaged_users - Вовлеченные пользователи Страницы за день
# page_views_total - просмотры страниц
# page_views_logged_in_unique - всего(уникальные), людей которые просмотрели
# page_positive_feedback_by_type - like, comment, link, other
metrics = 'page_impressions,page_impressions_unique,page_impressions_organic_unique,page_impressions_paid_unique,page_impressions_viral_unique,page_views_total' \
          ',page_engaged_users,page_views_logged_in_unique,page_positive_feedback_by_type'

currDate = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

l = [(datetime.date.today() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")]

for i in metrics.split(','):
    url = f'https://graph.facebook.com/v3.2/{page_ukt_id}/insights/?metric={i}&since={currDate}&until={currDate}&period={period}&access_token={Page_Access_Token}'
    cont = requests.get(url).json()
    if i != 'page_positive_feedback_by_type':
        l.append(cont['data'][0]['values'][0]['value'])
    else:
        l += [cont['data'][0]['values'][0]['value'].get('like', 0),
              cont['data'][0]['values'][0]['value'].get('link', 0),
              cont['data'][0]['values'][0]['value'].get('comment', 0),
              cont['data'][0]['values'][0]['value'].get('other', 0)]

l += ['Facebook']

labels = ['date', 'page_impressions', 'page_impressions_unique', 'page_impressions_organic_unique',
          'page_impressions_paid_unique', 'page_impressions_viral_unique', 'page_views_total', 'page_engaged_users', 'page_views_logged_in_unique', 'like', 'link',
          'comment', 'other', 'socialNet']  # порядок столбцов

df = pd.DataFrame.from_records([l], columns=labels)
gbq.to_gbq(df, 'Temp.SocialFacebook', '78997000000', if_exists='append')