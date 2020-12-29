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

    # blob.upload_from_filename(source_file_name)

    blobs = bucket.list_blobs()
    d=[]
    for blob in blobs:
        if blob.name.startswith('BQdata/2'): # фильтруем нужные нам файлы
            print(blob.name)
            d.append(blob.name)
    return d

z=list_blobs('ukt_owox')

for i in z:
    download_blob('ukt_owox', i, i[7:]) # i[7:] - это чисто имя файла, i - это полный путь к нему