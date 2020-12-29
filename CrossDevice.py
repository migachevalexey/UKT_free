import csv
import datetime
from builtins import range
from collections import defaultdict
import pandas as pd
from pandas.io import gbq
from pprint import pprint
from google.cloud import bigquery


PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'ukt'
import_action = 'replace'
projectid = '78997000000'
labels = ['data', 'user', 'sourceMedium', 'campaign']
client = bigquery.Client(project=PROJECT_ID)
dataset = client.dataset(DATASET_ID)

# смотрим buyer_id из APP когда он заходил перввый раз и когда он сделал первый заказа
# Потом в стриминге смотрим был ли он в этото период на сайте и с какого канала

z=[]
def userIdDict():
    with open('123.csv', 'a') as f:
        QUERY = "with a as (SELECT buyer_id, min(date) as zahod FROM `MatchedData.App_trackers_matched` group by 1)," \
                "b as (SELECT buyer_id, min(date) as zakaz FROM `MatchedData.App_trackers_matched` " \
                "where TransacID !='No data' group by 1)" \
                "select a.buyer_id, zahod,zakaz from a,b where a.buyer_id=b.buyer_id and zahod!=zakaz and zakaz>'2018-03-03' and DATE_DIFF(zakaz,zahod, DAY)<30"
        query_job = client.query(QUERY)  # API request
        rows = query_job.result() # Waits for query to finish
        writer = csv.writer(f)
        for row in rows:
            user, date_stop, date_start = row[0], row[1], row[2]
            writer.writerow([user, date_stop.strftime("%Y-%m-%d"), date_start.strftime("%Y-%m-%d")])
            query = 'select distinct user.id, date, device.deviceCategory, trafficSource.source, trafficSource.medium,trafficSource.campaign, sum(totals.transactions)  ' \
                    'from `ukt.owoxbi_sessions_*` where _TABLE_SUFFIX between "{}" and "{}" and user.id ="{}" group by 1,2,3,4,5,6 order by 2'.\
                format((row[1]-datetime.timedelta(days=1)).strftime("%Y%m%d"), (row[2]-datetime.timedelta(days=1)).strftime("%Y%m%d"), row[0])
            query_job1 = client.query(query)  # API request
            rows1 = query_job1.result()

            for row1 in rows1:
                # print([row1[i] for i in range(len(row1))])
                writer.writerow([row1[i] for i in range(len(row1))])


userIdDict()
