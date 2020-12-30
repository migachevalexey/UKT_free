from google.cloud import bigquery
client = bigquery.Client(project='glowing-cargo-000000')

table_ref = client.dataset('Mydata').table('My_Contact')
previous_rows = client.get_table(table_ref).num_rows
job_config = bigquery.LoadJobConfig()
job_config.source_format = 'NEWLINE_DELIMITED_JSON'
job_config.write_disposition = 'WRITE_APPEND'

load_job = client.load_table_from_uri(
    'gs://glowing-cargo-000000.appspot.com/temp/temp.json',
    table_ref,
    job_config=job_config)  # API request

assert load_job.state == 'RUNNING'
assert load_job.job_type == 'load'

load_job.result()  # Waits for table load to complete.

assert load_job.state == 'DONE'
assert client.get_table(table_ref).num_rows == previous_rows + 50
