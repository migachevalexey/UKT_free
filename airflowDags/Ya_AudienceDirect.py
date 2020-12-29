# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta, date
from airflow.models import Variable
# https://tech.yandex.ru/direct/doc/ref-v5/campaigns/get-docpage/
import time
import requests
import hashlib
import pprint
import MySQLdb
import json
import pandas as pd
import yaaudience

'''
2 раза в неделю формирует аудиторию(все наши покупатели) из ВИ сайта.
Отправляем эту аудиторию в сервис Яндекс.Аудитории оттуда пушим в Яндекс.Директ и навешиваем эти аудитории на нужные кампании
Список кампаний прилагается   
'''

now_date = date.today()
path = "/home/airflow/logs/YandexAudience/"

data_phone = path + f"{now_date}_phone.csv"
clear_data_phone = path + f"{now_date}_md5_phone.csv"
data_mail = path + f"{now_date}_mail.csv"
clear_data_mail = path + f"{now_date}_md5_mail.csv"
phoneNew = path + f"{now_date}_phoneNew.csv"
clear_phoneNew = path + f"{now_date}_md5_phoneNew.csv"
mailNew = path + f"{now_date}_mailNew.csv"
clear_mailNew = path + f"{now_date}_md5_mailNew.csv"
phoneSleep = path + f"{now_date}_phoneSleep.csv"
clear_phoneSleep = path + f"{now_date}_md5_phoneSleep.csv"
mailSleep = path + f"{now_date}_mailSleep.csv"
clear_mailSleep = path + f"{now_date}_md5_mailSleep.csv"

pathPass="/home/airflow/keys/connections_key.json"

urlAudience = 'https://api.direct.yandex.com/json/v5/retargetinglists'
urlBid = 'https://api.direct.yandex.com/json/v5/bidmodifiers'

token = 'Bearer ' + Variable.get('mgYandexToken')
ya_token = Variable.get("yandexTokenAudience")

ClientLogin = 'ukt.mg'
headers = {'Authorization': token, 'Client-Login': ClientLogin,
           'Accept-Language': 'ru', "Content-Type": "application/json; charset=utf-8"}

phone_segment_name = f'{now_date}_phone'
mail_segment_name = f'{now_date}_mail'
new_phone_segment = f'{now_date}_phone_NewUser'
new_mail_segment = f'{now_date}_mail_NewUser'
sleep_phone_segment = f'{now_date}_phone_SleepUser'
sleep_mail_segment = f'{now_date}_mail_SleepUser'


p_data_type = 'crm'

CampID = [37466000, 37466000, ]


def data_from_DB():

    db_connect = None
    with open(pathPass) as f:
        param_сonnect = json.load(f)
    db_connect = MySQLdb.connect(user=param_сonnect['user'], passwd=param_сonnect['passwd'],
                                 host=param_сonnect['host'], db=param_сonnect['db_sess'], charset='cp1251')

    db_export_phone = "select DISTINCT phone from sess_bon where phone regexp '^7' and phone_confirmed=1"
    db_export_mail = "SELECT DISTINCT mail_address as email from sess_bon WHERE mail_address !=''"

    db_export_phoneNew = "SELECT DISTINCT phone from sess_bon " \
                         "WHERE buyer_id IN (SELECT m1.buyer_id FROM ukt_mark_gr_co m1 " \
                         "LEFT JOIN ukt_mar_gr_coo m2 ON (m1.buyer_id=m2.buyer_id AND m1.id < m2.id) " \
                         "LEFT JOIN ukt_mar_gr m3 ON (m1.marketing_group_id=m3.id) " \
                         "WHERE m2.id IS NULL AND m3.alias ='NEW') and phone_confirmed=1"

    db_export_mailNew = "SELECT DISTINCT mail_address as email from sess_bon " \
                        "WHERE buyer_id IN (SELECT m1.buyer_id FROM ukt_mark_gr_co m1 " \
                        "LEFT JOIN ukt_mark_gr_co m2 ON (m1.buyer_id=m2.buyer_id AND m1.id < m2.id) " \
                        "LEFT JOIN ukt_mark_gr m3 ON (m1.marketing_group_id = m3.id) " \
                        "WHERE m2.id IS NULL AND m3.alias ='NEW') and mail_address!=''"

    db_export_phoneSleep = "SELECT DISTINCT phone from sess_bon " \
                           "WHERE buyer_id IN (SELECT m1.buyer_id FROM ukt_mark_gr_co m1 " \
                           "LEFT JOIN ukt_mark_gr_co m2 ON (m1.buyer_id=m2.buyer_id AND m1.id < m2.id) " \
                           "LEFT JOIN ukt_mark_gr m3 ON (m1.marketing_group_id=m3.id) " \
                           "WHERE m2.id IS NULL AND m3.alias ='SLEEP') and phone_confirmed=1"

    db_export_mailSleep = "SELECT DISTINCT mail_address as email from sess_bon " \
                          "WHERE buyer_id IN (SELECT m1.buyer_id FROM ukt_mark_gr_co m1 " \
                          "LEFT JOIN ukt_mark_gr_co m2 ON (m1.buyer_id=m2.buyer_id AND m1.id < m2.id) " \
                          "LEFT JOIN ukt_mark_gr m3 ON (m1.marketing_group_id = m3.id) " \
                          "WHERE m2.id IS NULL AND m3.alias ='SLEEP') and mail_address!=''"
    try:
        print("Start extracting data from DB")
        start = datetime.now()
        try:
            df = pd.read_sql(db_export_phone, con=db_connect)
            df_mail = pd.read_sql(db_export_mail, con=db_connect)
            df.to_csv(data_phone, sep=',',  index=False)
            df_mail.to_csv(data_mail, sep=',', index=False)

            df_phoneNew = pd.read_sql(db_export_phoneNew, con=db_connect)
            df_mailNew = pd.read_sql(db_export_mailNew, con=db_connect)
            df_phoneSleep = pd.read_sql(db_export_phoneSleep, con=db_connect)
            df_mailSleep = pd.read_sql(db_export_mailSleep, con=db_connect)

            df_phoneNew.to_csv(path+f"{now_date}_phoneNew.csv", sep=',', index=False)
            df_mailNew.to_csv(path+f"{now_date}_mailNew.csv", sep=',', index=False)
            df_phoneSleep.to_csv(path+f"{now_date}_phoneSleep.csv", sep=',', index=False)
            df_mailSleep.to_csv(path+f"{now_date}_mailSleep.csv", sep=',', index=False)
        finally:
            if db_connect is not None:
                db_connect.close()
        print("End extracting data from DB. Elasped time: " + str(datetime.now() - start))

        # Clearing and hashing extracted data
        print("Start clearing/hashing data")
        start = datetime.now()
        df = pd.read_csv(data_phone, dtype={"phone":object}).replace('[^\d.]+', "", regex=True).applymap(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()) #  любой символ, кроме тех, что в скобках
        df_mail = pd.read_csv(data_mail, dtype='str').applymap(lambda x: x.lower()).applymap(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()) # transform entire dataframe to lowercase

        # ТУТ СРАЗУ ПРЕОБРАЗУЕМ В lower И КОДИРУЕМ В MD5
        df_phoneNew = pd.read_csv(phoneNew, dtype={"phone":object}).replace('[^\d.]+', "", regex=True)
        df_phoneNew.phone = df.phone.astype(str)
        df_phoneNew = df_phoneNew.applymap(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest())
        df_mailNew = pd.read_csv(mailNew,  dtype={"phone":object}).applymap(lambda x: x.lower()).applymap(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest())
        df_phoneSleep = pd.read_csv(phoneSleep, dtype={"phone":object}).replace('[^\d.]+', "", regex=True)
        df_phoneSleep['phone'] = df_phoneSleep['phone'].astype(str)
        df_phoneSleep = df_phoneSleep.applymap(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest())
        df_mailSleep = pd.read_csv(mailSleep,  dtype={"phone":object}).applymap(lambda x: x.lower()).applymap(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest())


        df.to_csv(clear_data_phone, sep=',',  index=False)
        df_mail.to_csv(clear_data_mail, sep=',', index=False)
        df_phoneNew.to_csv(clear_phoneNew, sep=',', index=False)
        df_mailNew.to_csv(clear_mailNew, sep=',', index=False)
        df_phoneSleep.to_csv(clear_phoneSleep, sep=',', index=False)
        df_mailSleep.to_csv(clear_mailSleep, sep=',', index=False)

        print("End clearing/hashing data. Elasped time: " + str(datetime.now() - start))

    except Exception as e:
        print('!!! Unexpected error: ' + str(e))
    finally:
        print('FINISH')


# Sending data to Yandex.Audience
def dataToYandexAudience():

    try:
        print("Start uploading segment data")
        start = datetime.now()
        ya = yaaudience.YaAudience(token=ya_token, debug=False)
        ya_segment_confirmed = None
        newAudience={}

        with open(clear_data_phone, 'r') as data_phone, open(clear_data_mail, 'r') as data_mail, open(clear_phoneNew,
                                                                                                      'r') as phoneNew, open(
                clear_phoneSleep, 'r') as phoneSleep, open(clear_mailNew, 'r') as mailNew, open(clear_mailSleep,
                                                                                                  'r') as mailSleep:
            for i in [(data_phone, phone_segment_name), (data_mail, mail_segment_name), (phoneNew, new_phone_segment),
                      (mailNew, new_mail_segment), (phoneSleep, sleep_phone_segment), (mailSleep, sleep_mail_segment)]:
                ya_segment_uploaded = ya.segments_upload_file(i[0])
                # print(ya_segment_uploaded)
                ya_segment_confirmed = ya.segment_confirm(segment_id=ya_segment_uploaded.id,
                                                      segment_name=i[1],
                                                      content_type=p_data_type,
                                                      hashed=True)

                newAudience[ya_segment_confirmed.FIELDS['id']] = ya_segment_confirmed.FIELDS['name']
        print(newAudience)


        print("End uploading segment data. Elasped time: " + str(datetime.now() - start))
    except Exception as e:
        print('!!! Unexpected error: ' + str(e))

    return {k: v for k, v in newAudience.items() if v in [phone_segment_name, mail_segment_name]}  


def lastSegmentsYandexAudience():

    ya = yaaudience.YaAudience(token=ya_token)
    ya_segments = ya.segments()
    segmentStatus = [i.FIELDS['status'] for i in ya_segments if
                     i.FIELDS['name'] in [phone_segment_name, mail_segment_name]] 

    return segmentStatus


def segmentToDirect(newAudienceDict):
    NewSegmentsIdDirect=[]
    for segmentID, segmentName in newAudienceDict.items():
        AudienceAdd = {
        'method': 'add',  
        'params': {
            "RetargetingLists":
                [{
                    "Type": 'RETARGETING',
                    "Name": segmentName,
                    "Rules": [{
                        "Arguments": [{
                            "MembershipLifeSpan": 5,
                            "ExternalId": int('20'+str(segmentID))}],  # 2019-08-05_mail int('20'+str(campId))
                        "Operator": "ALL"}]}]}}

        response = requests.post(urlAudience, data=json.dumps(AudienceAdd), headers=headers).json()
        print(response)
        NewSegmentsIdDirect.append(response['result']['AddResults'][0]['Id'])

    return NewSegmentsIdDirect


def addBidToCamp(SegmentsId):
    for i in SegmentsId:
        for j in CampID:
            BidAdd = {'method': 'add',
                 "params": {
                     "BidModifiers": [{
                         "RetargetingAdjustments": [{
                             "RetargetingConditionId": i,  # id сегмента 2019-08-06_mail 2009752884 - это externalID
                             "BidModifier": 0}],
                         "CampaignId": j},]
                            }}

            response = requests.post(urlBid, data=json.dumps(BidAdd), headers=headers).json()
            print(response)


def main():
    data_from_DB()
    newAudience = dataToYandexAudience()
    segStatus = lastSegmentsYandexAudience()
    while True:
        if 'is_processed' not in segStatus:
            NewSegmentsIdDirect = segmentToDirect(newAudience)
            addBidToCamp(NewSegmentsIdDirect)
            break
        else:
            print('Сегмент не готов. Ждите!', time.strftime('%H:%M:%S'))
            time.sleep(1800)
            segStatus = lastSegmentsYandexAudience()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 19),
    'email': ['mig@ukt.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
}


dag = DAG(
    dag_id='AudienceToYaDirect',
    default_args=default_args, 
    schedule_interval='0 20 * * 6',
    description='Аудитории в Яндкекс.Аудитории -> Я.Директ '
)

task_1 = PythonOperator(
    task_id='Audience_to_YaDirect', python_callable=main, dag=dag)

task_2 = BashOperator(
    task_id='delete_old_files',
    bash_command='find /home/airflow/logs/YandexAudience/ -atime +20 -delete', dag=dag)

task_1 >> task_2

