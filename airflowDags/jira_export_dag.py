import sys
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow import DAG
sys.path.append('/home/airflow/python/')
# business imports
from Jira_function import basic_function
from goods_export import goods_export

dag = DAG('jira_export',
          description='Run export jira isussues.',
          schedule_interval='0 */6 * * *',
          start_date=datetime(2019, 12, 4), catchup=False)

step_1 = PythonOperator(task_id='step_1',
                        python_callable=basic_function,
                        op_kwargs={'prefix': 'CL'},
                        dag=dag)
step_2 = PythonOperator(task_id='step_2',
                        python_callable=basic_function,
                        op_kwargs={'prefix': 'CR'},
                        dag=dag)
step_3 = PythonOperator(task_id='step_3',
                        python_callable=goods_export,
                        dag=dag)

step_1 >> step_2 >> step_3
