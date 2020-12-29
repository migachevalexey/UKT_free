# import pytest
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
import re
import os
from multiprocessing.dummy import Pool as ThreadPool
from datetime import datetime
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="G:\Pkeys\keys\glowing-cargo-Yam.json"

"""
Удаляет таблицы в заданном DATASET_ID по заданным критериям - Регулярные выражения
"""

PROJECT_ID = 'glowing-cargo-144000'
DATASET_ID = 'Ufa'
regexp = 'streaming_2017'

client = bigquery.Client(project=PROJECT_ID)
datasets = list(client.list_datasets()) # параметр  dataset_id - имя dataset
dataset = client.dataset(DATASET_ID)
tables = list(client.list_tables(dataset))
kolvo_do = len(tables)

filtrTables = [i.table_id for i in tables if regexp in i.table_id]
#print(filtrTables)

def delTablesGBQ(table):
    table_ref = client.dataset(DATASET_ID).table(table)
    client.delete_table(table_ref)
    # with pytest.raises(NotFound):
    #   table_ref = dataset.table(table)
    #   table = client.get_table(table_ref)

n=40  # колво потоков
start = datetime.now()
pool = ThreadPool(n)
results = pool.map(delTablesGBQ, filtrTables)
pool.close()
pool.join()
print(datetime.now()-start)
kolvo_posle = len(list(client.list_tables(dataset)))
print(f'Было до удаления - {kolvo_do}\nУдалили - {kolvo_do - kolvo_posle}\nОсталось - {kolvo_posle}')
print(datetime.now()-start)


# для истории, через циклы
# for table in filtrTables:
#     table_ref = client.dataset(DATASET_ID).table(table)
#     client.delete_table(table_ref)
#     # with pytest.raises(NotFound):
#     #     table_ref = dataset.table(table)
#     #     table = client.get_table(table_ref)