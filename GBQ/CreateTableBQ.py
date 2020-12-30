import pprint

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField


'''
Создание таблицы на основе СУЩЕСТВУЮЩЕЙ таблицы
Тянем SCHEMA из существующей таблицы, изменяем ее и создаем на ее основе таблицу!
getSсhemaSource() - получение схемы таблицы, на основе которой нужно создать новую таблицу
createNewTable() - создает таблицу по заданной схеме(SCHEMA), имя таблицы указываем в TABLE_ID_Dest
# Можно добавлять\удалять поля, менять порядок полей, название и тип поля. Очень удобно, если сделал ошибку при создании таблицы
# имя новой таблицы указываем в TABLE_ID_Dest. 
Порядок запуска
При создании таблицы на основе существующей - вначале запускаем  getShemaSource()
 копируем результат в SCHEMA_COMMON(или SCHEMA_REPEATED), потом вносим нужные изменения и запускаем createNewTable()
'''

PROJECT_ID = 'glowing-cargo-000000' 
DATASET_ID = 'Mydata'
TABLE_ID_Source = 'emailK' # таблица-источник
TABLE_ID_Dest = 'email_dest'

client = bigquery.Client(project=PROJECT_ID)
dataset = client.dataset(DATASET_ID)

# обычная таблица
SCHEMA_COMMON =[SchemaField('name', 'string', 'NULLABLE', None, ()),
 SchemaField('f_name', 'string', 'NULLABLE', None, ()),
 SchemaField('email', 'string', 'NULLABLE', None, ()),
 SchemaField('phone', 'string', 'NULLABLE', None, ()),
 SchemaField('dr', 'date', 'NULLABLE', None, ()),
 SchemaField('gender', 'string', 'NULLABLE', None, ()),
 SchemaField('cities_id', 'integer', 'NULLABLE', None, ()),
 SchemaField('street', 'string', 'NULLABLE', None, ()),
 SchemaField('cities', 'string', 'NULLABLE', None, ()),
 ]


# таблица с вложенными полями
SCHEMA_REPEATED = [SchemaField('clientID', 'INTEGER', mode='NULLABLE'),
                   SchemaField('info', 'RECORD', mode='REPEATED',
                               fields=[SchemaField('phone', 'string', 'NULLABLE'),
                                       SchemaField('email', 'string', 'NULLABLE'),
                                       ]
                               )]

def getSсhemaSource():
    table_source = dataset.table(TABLE_ID_Source)
    table = client.get_table(table_source)
    pprint.pprint(table.schema)

    return table.schema

def createNewTable(SCHEMA):

    table_ref = dataset.table(TABLE_ID_Dest)
    table = bigquery.Table(table_ref, schema=SCHEMA)
    client.create_table(table)

def main():
    # step 1
    getSсhemaSource()
    # Step 2
    createNewTable(SCHEMA_COMMON) #указать нужную схему
    
main()
