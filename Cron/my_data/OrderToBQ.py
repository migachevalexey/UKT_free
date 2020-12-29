import datetime
from google.cloud import bigquery
import MySQLdb
import json
from pandas.io import gbq
import pandas as pd
import DataGa as gaa
from google.oauth2 import service_account
'''
Тянем данные по заказам из site DB, потом тянем заказы и источники из GA(через отдельный файл DataGa) 
мержим все это между собой и отправляем в GBQ. 
Тянем оплаченные деньги и статус отмены за последние 3 недели и обновляем эти поля в таблице Mig_Data.Orders
Через временную таблицу, ее вконце удаляем. 
Результат записываем в лог-файл
'''

PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'Mig_Data'
TABLE_Orders = 'Orders'
TABLE_Params = 'OrdersStatus'
file_db_connect = 'C:/Python/ukt/pass/MySQL_db_connect.json'
client = bigquery.Client(project=PROJECT_ID)
dataset = client.dataset(DATASET_ID)
projectid = '78997000000'

dateStart = str(datetime.date.today() - datetime.timedelta(days=1))

DateStartStatus = (datetime.date.today() - datetime.timedelta(days=22)).strftime("%Y-%m-%d")
DateStopStatus = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


def sql_db_select():
    with open(file_db_connect) as f:
        param_сonnect = json.load(f)
    db_connect = MySQLdb.connect(user=param_сonnect['user'], passwd=param_сonnect['passwd'],
                                 host=param_сonnect['host'], db=param_сonnect['db_sess'], charset='cp1251'
                                 )
    cursor = db_connect.cursor()
    sql_query = "select distinct z.master_order_id as transaction,z.BUYER_ID as kpp,summa,pay_sum as money,DATE(created) as date, EXTRACT(hour FROM created) as hour, TIME(created) as time," \
                "flag_cancel as canceled, if(fo.submitted_master_order_id is null,0,1) as firstOrder," \
                "case when sg.title in ('iPhone App','iPad App') then 'App iOS' " \
                "when sg.title in ('App - Android Mobile','App - Android Tablet', 'Android App') then 'App Android' " \
                "when sg.title in ('Mobile web - iPhone','MobileWeb-AndroidMob','MobileWeb-Android') then 'mobileWeb' " \
                "when sg.title in ('Call center') then 'CallCenter' " \
                "when sg.title in ('Чат-бот Алиса') then 'AlisaBot' " \
                "when sg.title in ('PC Brows','Desktop iPhone', 'Desktop iPad','DskWeb-AndroidMob','DsktpWeb-AndroidTabl') then 'Web' end as orderSource," \
                "p.title as paymentType, " \
                "dt.title as  shippingType," \
                "case  when sg.title in ('iPhone App','iPad App') then 'App iOS' " \
                "when sg.title in ('Android App') then 'App Android' " \
                "when sg.title in ('Mobile iPhone') then 'Mobile iOS' " \
                "when sg.title in ('MobileWeb-AndroidMob','MobileWeb-AndroidTab') then 'Mobile Android' " \
                "when sg.title in ('DesktopiPhone','DesktopiPad') then 'Desktop iOS' " \
                "when sg.title in ('DskWeb-Androb','DskWeb-ATabl') then 'Desktop Android' " \
                "when sg.title in ('Call center') then 'CallCenter' " \
                "when sg.title in ('Чат-бот Алиса') then 'AlisaBot' " \
                "when sg.title in ('PC Brow') then 'Web' end as simpleSource " \
                "from ZAKAZ z " \
                "left join ukt_first_order fo on (z.master_order_id=fo.submitted_master_order_id) " \
                "left join ukt_order_plac op on (op.pav_order_id=z.master_order_id) " \
                "left join ukt_sale_gr sg on (sg.sap_id=op.sale_group) " \
                "left join ukt_pay_type p on (z.prepay_flag=p.flag) " \
                "left join zak_deliv zd on (zd.pav_order_id=z.pav_order_id and z.archive=0) " \
                "left join ukt_deliv_type dt on (dt.id=zd.delivery_type_id) " \
                f"where date(created) between '{dateStart}' and '{dateStart}' and archive=0 and  sg.title!='Android old'"

    cursor.execute(sql_query)
    sql_data = cursor.fetchall()  # r=cursor.fetchmany(5)
    cursor.close()

    sql_status = "SELECT master_order_id as transaction,flag_cancel as cancel,pay_sum as money, summa as new_summa " \
                 "FROM ZAK " \
                "where Date(created) between '{}' and '{}' and archive=0".format(DateStartStatus, DateStopStatus)

    df_StatusMoney = pd.read_sql(sql_status, con=db_connect)

    db_connect.close()

    return list(list(i) for i in sql_data), df_StatusMoney


def stream_dataToBQ():
    ins_data, df_StatusMoneyUpdate = sql_db_select()
    # Python почему то при выгрузке из Mysql преобразует часы в time.delta. Тут делаем обратную операцию
    for i in ins_data:
          i[6] = (datetime.datetime.min + i[6]).time()

    labels = ['transaction', 'kpp', 'summa', 'money', 'date', 'hour', 'time', 'canceled',
              'firstOrder', 'orderSource', 'paymentType', 'shipping', 'simpleSource']

    dfSQL = pd.DataFrame.from_records(ins_data, columns=labels)
    dfGA = gaa.main(dateStart, dateStart)
    finalData = pd.merge(dfSQL, dfGA, how='left', on=['transaction'])

    finalData=finalData[['transaction', 'kpp', 'summa', 'date', 'money', 'orderSource', 'channel', 'medium', 'source', 'campaign',
                          'keyword', 'device', 'siteType', 'browser', 'firstOrder', 'time', 'canceled',
                          'hour', 'clientId', 'shipping', 'paymentType', 'city', 'userTypeGA', 'simpleSource']]

    gbq.to_gbq(finalData, f'{DATASET_ID}.{TABLE_Orders}', projectid, if_exists='append')  # отправка данных в GBQ
    gbq.to_gbq(df_StatusMoneyUpdate, f'{DATASET_ID}.{TABLE_Params}', '78997000000',if_exists='replace')
    with open(r'c:\python\ukt\Cron\log\OrdersFullToBQ.txt', 'a') as f:
        f.write(f'На дату: {dateStart} ' + f' загружено {str(len(finalData))} в таблицу {DATASET_ID}.{TABLE_Orders}'+'\n')

def queryData(QUERY):
    query_job = client.query(QUERY)
    rows = query_job.result()
    if QUERY.find('UPDATE') != -1:
        print(query_job.state, query_job.num_dml_affected_rows, "rows affected")
        with open(r'c:\python\ukt\Cron\log\OrdersFullToBQ.txt', 'a') as f:
            f.write(f'На дату: {dateStart} ' + f' обновлен summa,cancel_status,money у {str(query_job.num_dml_affected_rows)} записей в таблице {DATASET_ID}.{TABLE_Orders}' + '\n')
    return rows


def main():
    stream_dataToBQ()
    QUERY_upd_Status = f"UPDATE `{DATASET_ID}.{TABLE_Orders}` T SET T.canceled = S.cancel, T.money = S.money, T.summa = S.new_summa " \
                       f"FROM `{DATASET_ID}.{TABLE_Params}` S " \
                       f"WHERE T.transaction = S.transaction and (T.canceled != S.cancel or T.money != S.money or T.summa != S.new_summa)"
    queryData(QUERY_upd_Status)
    table_ref = client.dataset(DATASET_ID).table(TABLE_Params)
    if client.get_table(table_ref):
        client.delete_table(table_ref)

if __name__ == '__main__':
    main()

