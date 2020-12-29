#https://cloud.google.com/storage/docs/how-to

from google.cloud import bigquery, storage
import pandas as pd


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print('Blob {} downloaded to {}.'.format(
        source_blob_name,
        destination_file_name))


def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    blobs = bucket.list_blobs()

    for blob in blobs:
        print(blob.name)

# list_blobs('ukt_owox')


def rename_blob(bucket_name, blob_name, new_name):
    """Renames a blob."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    new_blob = bucket.rename_blob(blob, new_name)

    print('Blob {} has been renamed to {}'.format(
        blob.name, new_blob.name))

# rename_blob('ukt_owox', 'что то там/aaa.txt', 'что то там/bbb.txt')

import datetime

currDate = (datetime.date.today() - datetime.timedelta(days=3)).strftime("%Y-%m-%d")

storage_client = storage.Client()
bucket = storage_client.get_bucket('ukt_owox')
blob = bucket.blob(f'BQdata/YDSmartBanners_cost{currDate}.csv')


# needs an auth.json file as cloud auth not working for analytics requests

def upload_ga():
    import re


    df = pd.DataFrame(data=[{1,1.22,1.25},{2,1,22.3},{1,2,3.33}],columns=['id','name','description'])
    s = df.to_string(index=False)
    s=re.sub(" +", ",", s.strip())
    print(s.replace('\n,','\n'))

    # blob.upload_from_string(d)

upload_ga()