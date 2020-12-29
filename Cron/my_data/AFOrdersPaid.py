import pandas as pd
from pandas.io import gbq
from google.cloud import bigquery
import datetime

"""
Обновление source/Campaign у App заказов через temp-таблицу. Данные тянем из потока AppsFlyer
Результат пишем в общий лог-файл. Temp-таблицу в конце удаляем.
"""

PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'Mig_Orders'
import_action = 'replace'
projectid = '78997000000'
client = bigquery.Client(project=PROJECT_ID)

currdate = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


def queryData(QUERY):
    query_job = client.query(QUERY)
    rows = query_job.result()
    if QUERY.find('UPDATE') != -1:
        print(query_job.state, query_job.num_dml_affected_rows, "rows affected")
        with open(r'c:\python\ukt\Cron\log\OrdersFullToBQ.txt', 'a') as f:
            f.write(
                f'На дату: {currdate} ' + f' подтянут source\campaign у {str(query_job.num_dml_affected_rows)} записей по APP заказам в таблице Mig_Data.Orders' + '\n')
    return rows


def main():
    query_android = "SELECT distinct if (af_channel is not null, concat(media_source,'_',af_channel),media_source) as source," \
                    "ifnull(campaign,fb_campaign_name) as campaign, regexp_extract(event_value,'09835[0-9]{7}') as orders " \
                    "FROM AppsFlyer.android_events " \
                    "WHERE regexp_extract(event_value, '09835[0-9]{7}') is not null and media_source!='Organic' " \
                    f"and _PARTITIONTIME = TIMESTAMP('{currdate}')"

    query_ios = "SELECT distinct if (af_channel is not null, concat(media_source,'_',af_channel),media_source) as source," \
                "ifnull(campaign,fb_campaign_name) as campaign, regexp_extract(event_value,'09835[0-9]{7}') as orders " \
                "FROM AppsFlyer.ios_events " \
                "WHERE regexp_extract(event_value , '09835[0-9]{7}') is not null and media_source!='Organic' " \
                f"and _PARTITIONTIME = TIMESTAMP('{currdate}')"
    app = [[row[i] for i in range(3)] for row in queryData(query_android)] + [[row[i] for i in range(3)] for row in queryData(query_ios)]

    if len(app) > 0:
        # app = list(dict((x[2], x) for x in app).values())  # убираем дубли транзакций - оставил для истории )
        df_app = pd.DataFrame.from_records(app, columns=['source','campaign','orders'])
        df_app.drop_duplicates(subset='orders', inplace=True)  # убираем дубли транзакций
        gbq.to_gbq(df_app, 'Mig_Data.temp_app', projectid, if_exists=import_action)
        QUERY_upd = f"UPDATE `Mig_Data.Orders` o SET o.source = t.source, o.campaign=t.campaign " \
                    f"FROM `Mig_Data.temp_app` t " \
                    f"WHERE o.transaction = t.orders"
        queryData(QUERY_upd)
        table_ref = client.dataset('Mig_Data').table('temp_app')
        if client.get_table(table_ref):
            client.delete_table(table_ref)


if __name__ == '__main__':
    main()