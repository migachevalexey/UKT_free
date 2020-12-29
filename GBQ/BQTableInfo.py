from google.cloud import bigquery
import re

"""
свойства таблицы
"""

PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'ukt'
regexp = r'owoxbi_sessions_2018'

client = bigquery.Client(project=PROJECT_ID)
datasets = list(client.list_datasets()) # параметр  dataset_id - имя dataset
dataset = client.dataset(DATASET_ID)
tables = list(client.list_tables(dataset))

# print(tables[0].table_id, tables[1].table_id)

# Фильтруем список, что бы не делать лишних запросов в GBQ
filtrTables = [i for i in tables if i.table_id.startswith(regexp)]

for table_item in filtrTables:
    table = client.get_table(table_item.reference)
    # delta = datetime.timedelta(hours=3)
    print("Table {} Колво строк: {} Объем: {} Mb".format(
        table.table_id, table.num_rows,  round(table.num_bytes/1048576, 2)
            # num_bytes,num_rows,labels,description,modified,created
        ))