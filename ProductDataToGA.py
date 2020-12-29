import datetime
import MySQLdb
import json
import httplib2
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
import pandas as pd
import os

'''
Тянем данные по товарам из базы MySQL в csv файл и отправляем данные в Google Analytics
'''

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="../keys/konic-progress-196909-086050000000.json"
ACCOUNT_ID = '8149000'
UA_ID = 'UA-8149000-8'
DATA_IMPORT_ID = 'cR1pd8H9000000'
scope = ['https://www.googleapis.com/auth/analytics.edit']
service_account_email = 'ukt-cloud@konic-progress-196909.iam.gserviceaccount.com'
FILE_NAME = f'productData{datetime.date.today().strftime("%Y%m%d")}.csv'
runFileDir = os.path.dirname(__file__)
key_file_location = os.path.join(runFileDir, os.path.normpath(r'../keys/uktOwox-d00000000e12.p12'))
pathPass = os.path.join(runFileDir, os.path.normpath(r'../keys/connections_key.json'))
pathLog = os.path.join(runFileDir, os.path.normpath(fr'../logs/dataProductToGA/{FILE_NAME}'))

def sql_db_select():
    with open(pathPass) as f:
        param_сonnect = json.load(f)
    db_connect = MySQLdb.connect(user=param_сonnect['user'], passwd=param_сonnect['passwd'],
                                 host=param_сonnect['host'], db=param_сonnect['db_prod'], charset='cp1251'
                                 )

    sql_query = "select i.original_id as 'ga:productSku',replace(i.name,',','.') as 'ga:productName', replace(pv.value,',','.') as 'ga:productBrand'," \
                "replace(c.name,',','.') as 'ga:productCategoryHierarchy', if((pc.price - pc.price_purchase)>0,round((pc.price - pc.price_purchase)*100),0) as 'ga:metric1' " \
                "from PRICE_CAC pc inner join ITEM i on i.ITEM_ID = pc.MARKING_ID left join CATALOE c on c.id = i.catalogue_id " \
                "left join ukt_prop_mark pm on pm.item_id = i.item_id and pm.property_id = 479 " \
                "left join ukt_property_value pv on pv.id = pm.property_value_id where i.status in (5,6,7,8,9) and pc.active=1 and pc.date = date(now())"


    df_mysql = pd.read_sql(sql_query, con=db_connect)
    df_mysql['ga:productName'] = df_mysql['ga:productName'].str.replace('\n', '')
    df_mysql.to_csv(pathLog, index=False)


def get_service(api_name, api_version, scope, key_file_location,
                service_account_email):
    credentials = ServiceAccountCredentials.from_p12_keyfile(
        service_account_email, key_file_location, scopes=scope)
    http = credentials.authorize(httplib2.Http())
    service = build(api_name, api_version, http=http, cache_discovery=False)

    return service


def data_to_GA():
    service = get_service('analytics', 'v3', scope, key_file_location, service_account_email)

    media = MediaFileUpload(pathLog,
                            mimetype='application/octet-stream',
                            resumable=False)
    service.management().uploads().uploadData(
        accountId=ACCOUNT_ID,
        webPropertyId=UA_ID,
        customDataSourceId=DATA_IMPORT_ID,
        media_body=media).execute()


def main():
    sql_db_select()
    data_to_GA()


if __name__ == '__main__':
    main()