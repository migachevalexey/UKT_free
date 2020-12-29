# Constants and Parameters
import hashlib
import pprint

import MySQLdb
import json
import pandas as pd
import datetime
import yaaudience


now_date = datetime.date.today()
pathPass = r'C:\Python\ukt\pass\MySQL_db_connect.json'
with open(pathPass) as f:
    param = json.load(f)
ya_token = param['yandexTokenAudience']

data_phone = f"C:/Python/ukt/Cron/log/YandexAudience/{now_date}_phone.csv"
clear_data_phone = f"C:/Python/ukt/Cron/log/YandexAudience/{now_date}_hashmd5_phone.csv"
data_mail = f"C:/Python/ukt/Cron/log/YandexAudience/{now_date}_mail.csv"
clear_data_mail = f"C:/Python/ukt/Cron/log/YandexAudience/{now_date}_hashmd5_mail.csv"
file_db_connect = 'C:/Python/ukt/pass/MySQL_db_connect.json'

phone_segment_name = f'{now_date}_phone'
mail_segment_name = f'{now_date}_mail'

p_data_type = 'crm'
p_data_hashed = True  # use True of False


def data_from_DB():

    db_connect = None
    with open(file_db_connect) as f:
        param_сonnect = json.load(f)
    db_connect = MySQLdb.connect(user=param_сonnect['user'], passwd=param_сonnect['passwd'],
                                 host=param_сonnect['host'], db=param_сonnect['db_sess'], charset='cp1251')

    db_export_phone = "select DISTINCT phone from session_bonus where phone regexp '^7' and phone_confirmed=1"
    db_export_mail = "SELECT DISTINCT mail_address as email from session_bonus WHERE mail_address !=''"
    try:
        print('START')
        print("  Start extracting data from DB")
        start = datetime.datetime.now()
        try:
            df = pd.read_sql(db_export_phone, con=db_connect)
            df_mail = pd.read_sql(db_export_mail, con=db_connect)
            df.to_csv(data_phone, sep=',',  index=False)
            df_mail.to_csv(data_mail, sep=',', index=False)
        finally:
            if db_connect is not None:
                db_connect.close()
        print(" End extracting data from DB.  Elasped time: " + str(datetime.datetime.now() - start))

        # Clearing and hashing extracted data
        print("  Start clearing/hashing data")
        start = datetime.datetime.now()
        df = pd.read_csv(data_phone,  dtype='str')
        df = df.applymap(lambda x: x.lower())  # transform entire dataframe to lowercase
        df_mail = pd.read_csv(data_mail, dtype='str')
        df_mail = df_mail.applymap(lambda x: x.lower())  # transform entire dataframe to lowercase

        if p_data_type == 'crm':
            df = df.replace('[^\d.]+', '', regex=True) #  любой символ, кроме тех, что в скобках
        elif p_data_type == 'mac':
            df = df.replace('[;:,-\.]+', '', regex=True)

        if p_data_hashed:
            if p_data_type != 'mac':
                df = df.applymap(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest())
                df_mail = df_mail.applymap(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest())
            else:
                df = df.applymap(lambda x: hashlib.md5(bytes.fromhex(x)).hexdigest())

        df.to_csv(clear_data_phone, sep=',',  index=False)
        df_mail.to_csv(clear_data_mail, sep=',', index=False)
        print("   End clearing/hashing data.  Elasped time: " + str(datetime.datetime.now() - start))

    except Exception as e:
        print('!!! Unexpected error: ' + str(e))
    finally:
        print('FINISH')


# Sending data to Yandex.Audience
def dataToYandexAudience():
    try:
        print('START')
        print("  Start uploading segment data")
        start = datetime.datetime.now()
        ya = yaaudience.YaAudience(token=ya_token, debug=False)
        ya_segment_confirmed = None

        with open(clear_data_phone, 'r') as data_phone, open(clear_data_mail, 'r') as data_mail:
            for i in [(data_phone,phone_segment_name), (data_mail,mail_segment_name)]:
                ya_segment_uploaded = ya.segments_upload_file(i[0])
                print(ya_segment_uploaded)
                ya_segment_confirmed = ya.segment_confirm(segment_id=ya_segment_uploaded.id,
                                                      segment_name=i[1],
                                                      content_type=p_data_type,
                                                      hashed=p_data_hashed)

                print(ya_segment_confirmed)

        print("  End uploading segment data. Elasped time: " + str(datetime.datetime.now() - start))
    except Exception as e:
        print('!!! Unexpected error: ' + str(e))
    finally:
        print('FINISH')

# View segments inside Yandex.Audience
def allsegmentsYandexAudience():
     try:

        ya = yaaudience.YaAudience(token=ya_token)
        ya_segments = ya.segments()

        print("  Segments Count: ", ya_segments.__len__())
        print("  Segments Details:")
        for ya_segment in ya_segments:
            print(ya_segment)
            # print(ya_segment.FIELDS['id'], ya_segment.FIELDS['name'])
     except Exception as e:
        print('!!! Unexpected error: ' + str(e))
     finally:
        print('FINISH')


def lastSegmentsYandexAudience():

    ya = yaaudience.YaAudience(token=ya_token)
    ya_segments = ya.segments()
    lastSegments = {ya_segment.FIELDS['id']: ya_segment.FIELDS['name'] for ya_segment in ya_segments if
                    ya_segment.FIELDS['name'] in [phone_segment_name, mail_segment_name]}

    return lastSegments



# Delete existing segment
def delSegment(segment_id_for_delete):
    #segment_id_for_delete = '123456789'

    try:

        if (segment_id_for_delete is None or segment_id_for_delete == ''):
            raise Exception('You mast set SEGMENT_ID for deleting!!!')

        ya = yaaudience.YaAudience(token=ya_token)

        ya_is_segment_deleted = ya.segment_delete(segment_id=int(segment_id_for_delete))

        print('  Is segment deleted? ' + str(ya_is_segment_deleted))
    except Exception as e:
        print('!!! Unexpected error: ' + str(e))
    finally:
        print('FINISH')

def main():
    data_from_DB()
    dataToYandexAudience()
    # allsegmentsYandexAudience()
    # delSegment(9752695)

if __name__ == '__main__':
    main()