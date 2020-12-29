from collections import defaultdict
import pandas as pd
from pandas.io import gbq
from google.cloud import bigquery
import datetime

''' Тянем из Orders заказы, у которых Medium = null Смотрим их в стриминге по transactionID, если есть source/Medium - подставляем его в Orders 
    Далее тянем из Orders  - buyer_id, date по заказам, у которых Medium = null 
    Cмотрим в стриминге owox на эту дату по user.id ичточник\канал и делаем update таблицы Orders
    Здесь же делаем UPDATE Channel yandex.ru\Referral -> Organic    
'''

PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'ukt'
import_action = 'replace'
projectid = '78997000000'
client = bigquery.Client(project=PROJECT_ID)
dataset = client.dataset('Mig_Data')

DateStart = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
DateStop = DateStart
# DateStart = '2019-07-08'
# DateStop = '2019-07-13'

month = DateStart[5:7]
date_orders = (datetime.datetime.strptime(DateStart, '%Y-%m-%d') - datetime.timedelta(days=3)).strftime("%Y-%m-%d")


def queryData(QUERY, p=None):
    query_job = client.query(QUERY)
    rows = query_job.result()
    if QUERY.find('UPDATE') != -1:
        print(query_job.state, query_job.num_dml_affected_rows, "rows affected")
        with open(r'c:\python\ukt\Cron\log\OrdersFullToBQ.txt', 'a') as f:
            f.write(f'На дату: {DateStop} ' + f' подтянут source\medium у {str(query_job.num_dml_affected_rows)} записей в таблице Mig_Data.Orders {p}' + '\n')
    return rows


def OrderId():
    QUERY_OrdID = "SELECT transaction FROM `{}.Mig_Data.Orders` where regexp_contains(orderSource,'Web') " \
                  "and Medium is null and date>'{}'".format(PROJECT_ID, date_orders)
    Query_Source = 'SELECT distinct hits.transaction.transactionId as transactionId, clientId, if( visitNumber=1,"New Visitor","Returning Visitor") as userTypeGA, trafficSource.source, trafficSource.medium, trafficSource.campaign, trafficSource.keyword, t.device.browser as browser, t.device.deviceCategory as deviceCategory, geoNetwork.city, hits.transaction.affiliation as affiliation ' \
                   'FROM `{0}.{1}.owoxbi_sessions_2019{2}*` as t, unnest(hits) as hits ' \
                   'where hits.transaction.transactionId in UNNEST({3})' \
                   'and trafficSource.source!="site" and trafficSource.medium!="banner" and eCommerceAction.action_type="purchase"'.format(
        PROJECT_ID, DATASET_ID, month, [row[0] for row in queryData(QUERY_OrdID)])

    z = [[row[i] for i in range(11)] for row in queryData(Query_Source)]

    if len(z) > 0:
        df = pd.DataFrame.from_records(z, columns=['orders', 'clientId','userTypeGA','source', 'Medium', 'campaign','keyword','browser','deviceCategory','city', 'affiliation'])
        df.drop_duplicates(subset='orders', inplace=True)
        gbq.to_gbq(df, 'Mig_Data.temp_order', projectid, if_exists=import_action)
        QUERY_upd = f"UPDATE `Mig_Data.Orders` o  SET o.source = t.source,o.Medium = t.Medium, o.campaign=t.campaign," \
                    f"o.clientId = t.clientId ,o.userTypeGA =t.userTypeGA, o.browser=t.browser, o.device=t.deviceCategory,  o.city=t.city, o.siteType=t.affiliation " \
                    f"FROM `Mig_Data.temp_order` t  WHERE o.transaction = t.orders "
        queryData(QUERY_upd, p='через функцию orderId')
        QUERY_referral = "UPDATE `Mig_Data.Orders` set source='yandex', Medium='organic', channel='Organic Search' where source='yandex.ru' and medium='referral'"
        queryData(QUERY_referral, p='Channel yandex.ru\Referral to Organic')
        table_ref = client.dataset('Mig_Data').table('temp_order')
        if client.get_table(table_ref):
            client.delete_table(table_ref)
    else:
        print("Записей нет!")


def userIdDict():
    d = defaultdict(list)
    QUERY = "SELECT  date, kpp  FROM `{}.Mig_Data.Orders` where  regexp_contains( orderSource ,'Web') and date between '{}' and '{}' " \
            "and Medium is null group by 1,2 having count(kpp)=1".format(PROJECT_ID, DateStart, DateStop)
    for row in queryData(QUERY, p=None):
        key, val = row.date, row.kpp
        d[key].append(val)
    return d


def userIdData():
    p = userIdDict()
    for dt, u_id in p.items():
        try:
            QUERY = "with a as ( SELECT distinct date, user.id, " \
                    "trafficSource.source,  trafficSource.medium, trafficSource.campaign, clientId, if( visitNumber=1,'New Visitor','Returning Visitor') as userTypeGA," \
                    "trafficSource.keyword, t.device.browser as browser, t.device.deviceCategory as deviceCategory, geoNetwork.city," \
                    "count(user.id)  OVER (PARTITION BY user.id  order by user.id desc ) cont_number " \
                    "FROM `{0}.{1}.owoxbi_sessions_{2}` as t where safe_cast( user.id as int64) in UNNEST({3})) " \
                    "select * from a where cont_number=1".format(PROJECT_ID, DATASET_ID, dt.replace('-',''), u_id)
            z = [[row[i] for i in range(11)] for row in queryData(QUERY)]
            df = pd.DataFrame.from_records(z, columns=['date','user','source','Medium','campaign','clientId','userTypeGA','keyword','browser','deviceCategory','city'])
            gbq.to_gbq(df, 'Mig_Data.temp', projectid, if_exists=import_action)  # отправка данных в GBQ
            QUERY_update = "UPDATE `{0}.Mig_Data.Orders` as a set a.source=t.source, a.Medium=t.Medium, a.campaign=t.campaign, a.clientId = t.clientId, a.userTypeGA =t.userTypeGA, a.keyword=t.keyword," \
                           "a.browser=t.browser, a.device=t.deviceCategory, a.city=t.city " \
                           "from `{0}.Mig_Data.temp` as t " \
                           "where a.date=t.date and kpp=safe_cast(user as int64)".format(PROJECT_ID)
            queryData(QUERY_update, p='через функцию userId')
            table_ref = client.dataset('Mig_Data').table('temp')
            if client.get_table(table_ref):
                client.delete_table(table_ref)
        except:
            continue


def main():
    OrderId()
    userIdData()

if __name__ == '__main__':
    main()