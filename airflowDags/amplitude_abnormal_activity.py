"""
Check abnormal activity amplitude-events in mobile platforms
#1 send sql request to raw amplitude-data in oracle
#2 check amnormal events
#3 send to spreadsheet
https://docs.google.com/spreadsheets/d/1cZJfUev4UX7yczat-16EP21wipbcAH_rzxgT9BDAcZg/edit#gid=1568972927
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from slack_operator import task_fail_slack_alert

#from __future__ import print_function

# For connect to google sheet
import warnings
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine

warnings.filterwarnings("ignore")
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import df2gspread as d2g


def request_sql(events, oracle_pass, oracle_login, oracle_host):
    engine = create_engine(
        f'oracle+cx_oracle://{oracle_login}:{oracle_pass}@{oracle_host}/?service_name=DWH?encoding=utf8')
    today = datetime.strftime(datetime.now() - timedelta(days=3), "%Y-%m-%d")

    to_sql = engine.execute(
        f'''
        with T1 AS (
        select  os_name,  EVENT_TYPE, COUNT(*) AS COUNTS
        from DWH_STAGE.AMPLITUDE_EVENTS
        WHERE "date" = to_date('{today}','yyyy-mm-dd')
        AND os_name != 'NULL'
        GROUP BY  os_name,  EVENT_TYPE
        ),
            T2 AS (
                select os_name, EVENT_TYPE, count(distinct device_id) AS USERS
                from DWH_STAGE.AMPLITUDE_EVENTS
                WHERE "date" = to_date('{today}','yyyy-mm-dd')
                AND os_name != 'NULL'
                GROUP BY os_name, EVENT_TYPE
            )
    
        SELECT T1.*, USERS, ROUND(COUNTS/USERS, 2 ) as DELTA
        FROM T1,T2
        WHERE
        T1.OS_NAME = T2.OS_NAME
        and  T1.EVENT_TYPE = T2.EVENT_TYPE
        '''
    )
    print('SQL SENT TO ORACLE')

    for row in to_sql:
        events.setdefault(row['event_type'], []).append({row['os_name']: row['delta']})
    return events


# Check sql-response non events
def sql_empty_test(events):
    sql_empty = True
    if len(events) <= 0:
        sql_empty = False
    assert sql_empty, 'SQL IS EMPTY'


def scoring_events(events, totals_deflection):
    # Scoring events between platforms
    not_in_platform = {}

    for event_name in events:
        ios = 0
        android = 0
        deflection = 0
        for os_name in events[event_name]:
            for key, value in os_name.items():
                if key == 'ios':
                    ios = value
                else:
                    android = value

        if android == 0:
            not_in_platform[event_name] = 'only_in_ios'
        elif ios == 0:
            not_in_platform[event_name] = 'only_in_android'
        else:
            deflection = abs(int((android / ios * 100) - 100))
            if deflection > 30:
                totals_deflection[event_name] = deflection

    # Merge dicts
    totals_deflection.update(not_in_platform)
    return totals_deflection


def send_to_sheets(totals_deflection, spreadsheet_key, path_to_spred_key):
    df_to_sheets = pd.DataFrame(list(totals_deflection.items()), columns=['Event', 'DELTA'])

    # Configure the connection
    scope = ['https://spreadsheets.google.com/feeds']

    # Give the path to the Service Account Credential json file
    credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_spred_key + 'creds_gsheets.json', scope)

    # Authorise
    gc = gspread.authorize(credentials)

    # The sprad sheet ID, which can be taken from the link to the sheet
    wks_name = 'Sheet1'
    cell_of_start_df = 'A1'

    # upload the dataframe of the clients we want to delete
    d2g.upload(df_to_sheets,
               spreadsheet_key,
               wks_name,
               credentials=credentials,
               col_names=True,
               row_names=False,
               start_cell=cell_of_start_df,
               clean=False)
    print('The sheet is updated successfully')


def generate_amplitude_abnormal_activity_report(**kwargs):
    events = {}
    totals_deflection = {}
    oracle_pass = kwargs['oracle_pass']
    oracle_login = kwargs['oracle_login']
    oracle_host = kwargs['oracle_host']
    spreadsheet_key = kwargs['spreadsheet_key']
    path_to_spred_key = kwargs['path_to_spred_key']

    request_sql(events, oracle_pass, oracle_login, oracle_host)
    sql_empty_test(events)
    scoring_events(events, totals_deflection)
    send_to_sheets(totals_deflection, spreadsheet_key, path_to_spred_key)


params = {
    'oracle_pass': BaseHook.get_connection("DWH_DEE").password,
    'oracle_login': BaseHook.get_connection("DWH_DEE").login,
    'oracle_host': BaseHook.get_connection("DWH_DEE").host,
    'spreadsheet_key': '123',
    'path_to_spred_key': '/home/airflow/keys/'
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 30),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=15),
    'on_failure_callback': task_fail_slack_alert,
}

dag = DAG(
    dag_id='amplitude_abnormal_activity',
    default_args=default_args,
    schedule_interval='0 12 * * 6',
    description='Check_abnormal_events',
)

with dag:
    t1 = PythonOperator(
        task_id="amplitude_abnormal_activity_report",
        python_callable=generate_amplitude_abnormal_activity_report,
	op_kwargs=params
    )


