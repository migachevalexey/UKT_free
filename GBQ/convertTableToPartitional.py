from google.cloud import bigquery

"""
Конвертируем обычную таблицу с датой в _PARTITIONTIME TABLE, 
предварительно НУЖНО создать пустую PARTITIONTIME TABLE
"""

PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'Mig_Data'
TABLE = 'PartOrders'
client = bigquery.Client(project=PROJECT_ID)
dataset = client.dataset(DATASET_ID)

def dataInPartTable(dt):
    job_config = bigquery.QueryJobConfig()
    job_config.destination = dataset.table(TABLE + '${}'.format(dt.replace('-', '')))
    job_config.write_disposition = 'WRITE_APPEND'
    client.query(
        'SELECT * FROM `konic-progress-196909.Mig_Data.Orders` where date = "{}"'.format(dt), job_config=job_config)

query_job = client.query("select distinct date from `konic-progress-196909.Mig_Data.Orders`")  # API request
rows = query_job.result()  # Waits for query to finish

for i in [row['date'] for row in rows]:
    dataInPartTable(i)