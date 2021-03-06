#!/usr/bin/python
import gzip
import json
import os
import smtplib
import zipfile
import requests as re
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from google.cloud import bigquery, storage
from os import walk

ACCOUNT_ID = '247000'
API_KEY =  'api_key_amplitude из файла'
API_SECRET ='API_SECRET из файла'
PROJECT_ID = 'konic-progress-196909'
CLOUD_STORAGE_BUCKET = 'ukt_owox'
PROPERTIES = ["event_properties", "data", "groups", "group_properties",
              "user_properties"]


YESTERDAY = (datetime.utcnow().date() - timedelta(days=1)).strftime("%Y%m%d")
PATH = "amplitude/{id}/".format(id=ACCOUNT_ID)
bigquery_client = bigquery.Client(project=PROJECT_ID)
storage_client = storage.Client()
dataset_ref = bigquery_client.dataset('amplitude')


def remove_file(file, folder=''):
    folder = folder if folder == '' else folder + '/'
    os.remove("{folder}{file}".format(folder=folder, file=file))


def unzip_file(filename, extract_to):
    zip_ref = zipfile.ZipFile(filename, 'r')
    zip_ref.extractall(extract_to)
    zip_ref.close()


def file_list(extension):
    files = []
    for (dirpath, dirnames, filenames) in walk("amplitude/{id}".format(id=ACCOUNT_ID)):
        for filename in filenames:
            if filename.endswith(extension):
                files.append(filename)
    return files


def file_json(filename):
    return filename.replace('.gz', '')


def unzip_gzip(filename, remove_original=True):
    f = gzip.open(filename, 'rb')
    file_content = f.read()
    f.close()

    with open(file_json(filename), 'wb') as unzipped_file:
        unzipped_file.write(file_content)

    if remove_original:
        remove_file(filename)


def upload_file_to_gcs(filename, new_filename, folder=''):
    folder = folder if folder == '' else folder + '/'
    bucket = storage_client.get_bucket(CLOUD_STORAGE_BUCKET)
    blob = bucket.blob('{folder}{file}'.format(folder=folder,
                                               file=new_filename))
    blob.upload_from_filename(filename)


def import_json_url(filename):
    return "gs://" + CLOUD_STORAGE_BUCKET + f"/import/{YESTERDAY}/" + filename


def value_def(value):
    value = None if value == 'null' else value
    return value


def value_paying(value):
    value = False if value is None else value
    return value


def load_into_bigquery(file, table):
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.ignore_unknown_values = True
    job_config.max_bad_records = 50
    job_config.source_format = 'NEWLINE_DELIMITED_JSON'
    job = bigquery_client.load_table_from_uri(import_json_url(file_json(file)),
                                              dataset_ref.table(table),
                                              job_config=job_config)
    print(job.result(), job.output_rows, "rows affected") # job.result()
    assert job.job_type == 'load'
    assert job.state == 'DONE'


def process_line_json(line):
    parsed = json.loads(line)
    if parsed:
        data = {}
        properties = []
        data['client_event_time'] = value_def(parsed['client_event_time'])
        data['ip_address'] = value_def(parsed['ip_address'])
        data['library'] = value_def(parsed['library'])
        data['dma'] = value_def(parsed['dma'])
        data['user_creation_time'] = value_def(parsed['user_creation_time'])
        data['insert_id'] = value_def(parsed['$insert_id'])
        data['schema'] = value_def(parsed['$schema'])
        data['processed_time'] = "{d}".format(d=datetime.utcnow())
        data['client_upload_time'] = value_def(parsed['client_upload_time'])
        data['app'] = value_def(parsed['app'])
        data['user_id'] = value_def(parsed['user_id'])
        data['city'] = value_def(parsed['city'])
        data['event_type'] = value_def(parsed['event_type'])
        data['device_carrier'] = value_def(parsed['device_carrier'])
        data['location_lat'] = value_def(parsed['location_lat'])
        data['event_time'] = value_def(parsed['event_time'])
        data['platform'] = value_def(parsed['platform'])
        data['is_attribution_event'] = value_def(parsed['is_attribution_event'])
        data['os_version'] = value_def(parsed['os_version'])
        data['paying'] = value_paying(parsed['paying'])
        data['amplitude_id'] = value_def(parsed['amplitude_id'])
        data['device_type'] = value_def(parsed['device_type'])
        data['sample_rate'] = value_def(parsed['sample_rate'])
        data['device_manufacturer'] = value_def(parsed['device_manufacturer'])
        data['start_version'] = value_def(parsed['start_version'])
        data['uuid'] = value_def(parsed['uuid'])
        data['version_name'] = value_def(parsed['version_name'])
        data['location_lng'] = value_def(parsed['location_lng'])
        data['server_upload_time'] = value_def(parsed['server_upload_time'])
        data['event_id'] = value_def(parsed['event_id'])
        data['device_id'] = value_def(parsed['device_id'])
        data['device_family'] = value_def(parsed['device_family'])
        data['os_name'] = value_def(parsed['os_name'])
        data['adid'] = value_def(parsed['adid'])
        data['amplitude_event_type'] = value_def(parsed['amplitude_event_type'])
        data['device_brand'] = value_def(parsed['device_brand'])
        data['country'] = value_def(parsed['country'])
        data['device_model'] = value_def(parsed['device_model'])
        data['language'] = value_def(parsed['language'])
        data['region'] = value_def(parsed['region'])
        data['session_id'] = value_def(parsed['session_id'])
        data['idfa'] = value_def(parsed['idfa'])

        # Loop through DICTs and save all properties
        for property_value in PROPERTIES:
            for key, value in parsed[property_value].items():
                value = 'True' if value is True else value
                value = 'False' if value is False else value
                properties.append({'property_type': property_value,
                                   'insert_id': value_def(parsed['$insert_id']),
                                   'key': value_def(key),
                                   'value': str(value)})

    return json.dumps(data), properties

def sendMail(l):


    HOST = "smtp.yandex.ru"
    SUBJECT = f"Streaming ot {YESTERDAY} proshel NE polnostiu.{l} NE zagruzilsya"
    TO = "mig@ukt.ru"
    FROM = "mapl333@yandex.ru"
    text = f"Zagruzka sreaming ot {YESTERDAY} proshla NE polnosty. File {l} NE zagruzilsa."
    BODY = "\r\n".join((
        f"From: {FROM}",
        f"To: {TO}",
        f"Subject: {SUBJECT}",
        "", text))

    server = smtplib.SMTP_SSL(HOST, 465)
    server.login('mapl333@yandex.ru', 'faqq141178')
    server.sendmail(FROM, [TO], BODY)
    server.quit()


def main():
    r = re.get(" https://amplitude.com/api/2/export?start=" + YESTERDAY + "T01&end="
               + YESTERDAY + "T23", auth=HTTPBasicAuth(API_KEY, API_SECRET))
    with open('amplitude.zip', 'wb') as f:
        f.write(r.content)

    dir=r'amplitude/import'
    filelist = [f for f in os.listdir(dir)]
    for f in filelist:
        os.remove(os.path.join(dir, f))

    dir=f'amplitude/{ACCOUNT_ID}'
    filelist = [f for f in os.listdir(dir)]
    for f in filelist:
        os.remove(os.path.join(dir, f))

    # Unzip the file
    unzip_file('amplitude.zip', 'amplitude')
    upload_file_to_gcs('amplitude.zip', YESTERDAY + '.zip', 'export')

    # Loop through all new files, unzip them & remove the .gz
    for file in file_list('.gz'):
        print("Parsing file: {name}".format(name=file))
        unzip_gzip(PATH + file)

        with open(PATH + file_json(file)) as f:
            lines = f.readlines()
        lines = [x.strip() for x in lines if x]

        # Create a new JSON import file
        import_events_file = open("amplitude/import/" + file_json(file), "w+")
        import_properties_file = open("amplitude/import/" + "properties_" +
                                      file_json(file), "w+")

        # Loop through the JSON lines
        for line in lines:
            events_line, properties_lines = process_line_json(line)
            import_events_file.write(events_line + "\n")
            for property_line in properties_lines:
                # тут добавил проверку иначе ломалась schema-table при type('value')=list переключалось с NULLABLE в required
                if isinstance(property_line['value'], list):
                    property_line['value'] = ';'.join(property_line['value'])
                import_properties_file.write(json.dumps(property_line) + "\n")


        # Close the file and upload it for import to Google Cloud Storage
        import_events_file.close()
        import_properties_file.close()
        upload_file_to_gcs("amplitude/import/" + file_json(file), file_json(file),
                           f'import/{YESTERDAY}')
        upload_file_to_gcs(f"amplitude/import/" + "properties_" + file_json(file),
                           "properties_" + file_json(file), f'import/{YESTERDAY}')

        try:
            load_into_bigquery(file, 'events$' + YESTERDAY)
            load_into_bigquery("properties_" + file, 'events_properties$' + YESTERDAY)
            remove_file(file_json(file), "amplitude/import")
            remove_file("properties_" + file_json(file), "amplitude/import")
            print("Imported: {file}".format(file=file_json(file)))

        except:
            sendMail(file)
            continue

        # Remove JSON file
        remove_file(file_json(file), f"amplitude/{ACCOUNT_ID}")

    # Remove the original zipfile
    remove_file("amplitude.zip")


if __name__ == '__main__':
    main()
