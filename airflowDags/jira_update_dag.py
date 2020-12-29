import sys
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow import DAG
# business imports
sys.path.append('/home/airflow/python/')
from cl_cr_update import jira_update

dag = DAG('cl_cr_update',
          description='Run update jira isussues.',
          schedule_interval='*/5 * * * *',
          start_date=datetime(2019, 12, 7), catchup=False)

step_1 = PythonOperator(task_id='step_1',
                        python_callable=jira_update,
                        dag=dag)
