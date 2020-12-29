import csv
import time
import json
import pprint
import requests
import random, string
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
import httplib2

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 3),
    'email': ['mig@ukt.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
	}

dag = DAG(
    dag_id='YandexBannerCost', 
    default_args=default_args, 
    schedule_interval='0 6 * * *', 
    description='Расходы в GA по Яндкекс баннерам' )

scope = ["https://www.googleapis.com/auth/analytics.edit"]
service_account_email = 'ukt-cloud@konic-progress-196909.iam.gserviceaccount.com'
key_file_location = r'/home/airflow/keys/uktOwox-d00000000e12.p12'
ACCOUNT_ID = '8149000'
UA_ID =  Variable.get('UA_ID')
DATA_IMPORT_ID = 'B871d6-000000-000000000'
ReportsURL = 'https://api.direct.yandex.com/json/v5/reports'

token = 'Bearer ' + Variable.get('mgYandexToken')
ClientLogin = 'ukt.mg'

headers = {'Authorization': token, 'Client-Login': ClientLogin,
           'Accept-Language': 'ru', "processingMode": "auto", 'returnMoneyInMicros': 'false',
           'skipReportHeader': 'true', 'skipColumnHeader': 'true',
           'skipReportSummary': 'true'}


nameReport = ''.join([random.choice(string.ascii_letters + string.digits) for n in range(9)])
path_file = "/home/airflow/logs/YandexBannerCost/YDSmartBanners_cost_{}.csv"
camId =[37813000, 37814200,  40657000]


def API_dataReceipt(dayAgo3):
    body = {
    "params": {
        "SelectionCriteria": {
            "DateFrom": dayAgo3,
            "DateTo": dayAgo3,
            "Filter": [
                {
                    "Field": "CampaignId",
                    "Operator": "IN",
                    "Values": camId
                },
                {
                    "Field": "Clicks",
                    "Operator": "GREATER_THAN",
                    "Values": [0]
                }
            ]
        },
        "FieldNames": [
            "Date",
            "Cost",
            'Clicks',
            "Impressions",

            "CampaignName",
            "AdGroupName",
            'CriterionType',
            "CampaignId",
        ],
        "ReportName": nameReport,
        "ReportType": "CUSTOM_REPORT",
        "DateRangeType": "CUSTOM_DATE",
        "Format": "TSV",
        "IncludeVAT": "NO",
        "IncludeDiscount": "NO"
    }
}
    bodyJson = json.dumps(body, indent=4)

    while True:
        req = requests.post(ReportsURL, bodyJson, headers=headers)

        if req.status_code == 400:
            print("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
            print("RequestId: {}".format(req.headers.get("RequestId", False)))
            print("JSON-код ответа сервера: \n{}".format(req.json()))
            break
        elif req.status_code == 200:
            print("Отчет создан успешно")
            break
        elif req.status_code == 201:
            print("Отчет успешно поставлен в очередь в режиме офлайн")
            retryIn = int(req.headers.get("retryIn", 15))
            print("Повторная отправка запроса через {} секунд".format(retryIn))
            time.sleep(retryIn)
        elif req.status_code == 202:
            print("Отчет формируется в режиме офлайн")
            retryIn = int(req.headers.get("retryIn", 15))
            print("Повторная отправка запроса через {} секунд".format(retryIn))
            time.sleep(retryIn)

    return req.text.splitlines()  # разбиваем строку в список по \n


def transformData(costData,dayAgo3):
    
    listListData = [list(i.split('\t')) for i in costData]
    for i in listListData:
        i[0] = i[0].replace('-', '')
        i[4] = i[4]+'|'+i[-1]
        # i[1]=round(float(i[1])*1.18,2)
        i.insert(1, 'cpc')
        i.insert(2, 'yandex')
    pprint.pprint(listListData[0:10])
    df = pd.DataFrame(listListData, columns=['ga:date', 'ga:medium', 'ga:source', 'ga:adCost', 'ga:adClicks','ga:impressions','ga:campaign','ga:adContent','ga:keyword',"camp"])
    df.drop(columns=["camp"], inplace=True)
    df.to_csv(path_file.format(dayAgo3), index=False)


def get_service(api_name, api_version, scope, key_file_location,
                service_account_email):
    credentials = ServiceAccountCredentials.from_p12_keyfile(
        service_account_email, key_file_location, scopes=scope)
    http = credentials.authorize(httplib2.Http())
    service = build(api_name, api_version, http=http, cache_discovery=False)
    return service


def data_to_GA(dayAgo3):
    service = get_service('analytics', 'v3', scope, key_file_location, service_account_email)
    media = MediaFileUpload(path_file.format(dayAgo3), mimetype='application/octet-stream', resumable=False)
    service.management().uploads().uploadData(accountId=ACCOUNT_ID, webPropertyId=UA_ID,
                                               customDataSourceId=DATA_IMPORT_ID,
                                               media_body=media).execute()

def main(dayAgo3):
    API_data = API_dataReceipt(dayAgo3)
    transformData(API_data,dayAgo3)
    data_to_GA(dayAgo3)

task = PythonOperator(
        task_id="costToGA", 
	# provide_context=True,
        op_args =['{{ macros.ds_add(ds, -3) }}'],
        python_callable=main,
        dag=dag,
    )

task_1 = BashOperator(
    task_id='delete_old_files',
    bash_command='find /home/airflow/logs/YandexBannerCost/ -atime +20 -delete', dag=dag)

task >> task_1
