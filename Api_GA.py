

#https://habr.com/ru/company/idfinance/blog/457052/
from apiclient.discovery import build
import os
import unicodecsv as csv
from googleapiclient.errors import HttpError
import time

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'my_key_file.p12'
SERVICE_ACCOUNT_EMAIL = 'service_account_email@my-google-cloud-project.iam.gserviceaccount.com'


class ApiGA():

    def __init__(self, scopes=SCOPES,
                 key_file_location=KEY_FILE_LOCATION,
                 service_account_email=SERVICE_ACCOUNT_EMAIL,
                 version='v4'):
        credentials = ServiceAccountCredentials.from_p12_keyfile(
            service_account_email, key_file_location, scopes=scopes)
        self.handler = build('analytics', version, credentials=credentials)

    def downloadReport(self, view_id, dim_list, metrics_list, date, page, end_date=None, filters=None):
        if not end_date:
            end_date = date
        body = {
            'reportRequests': [
                {
                    'viewId': view_id,
                    'dateRanges': [{'startDate': date, 'endDate': end_date}],
                    'dimensions': dim_list,
                    'metrics': metrics_list,
                    'includeEmptyRows': True,
                    'pageSize': 10000,
                    'samplingLevel': 'LARGE'
                }]}
        if page:
            body['reportRequests'][0]['pageToken'] = page
        if filters:
            body['reportRequests'][0]['filtersExpression'] = filters
        while True:
            try:
                return self.handler.reports().batchGet(body=body).execute()
            except HttpError:
                time.sleep(0.5)

    def getData(self, view_id, dimensions, metrics, date, filename='raw_data.csv', end_date=None, write_mode='wb',
                filters=None):
        dim_list = map(lambda x: {'name': 'ga:' + x}, dimensions)
        metrics_list = map(lambda x: {'expression': 'ga:' + x}, metrics)

        file_data = open(filename, write_mode)
        writer = csv.writer(file_data)

        page = None
        while True:
            response = self.downloadReport(view_id, dim_list, metrics_list, date, page, end_date=end_date,
                                           filters=filters)
            report = response['reports'][0]
            rows = report.get('data', {}).get('rows', [])
            for row in rows:
                dimensions = row['dimensions']
                metrics = row['metrics'][0]['values']
                writer.writerow(dimensions + metrics)

            if 'nextPageToken' in report:
                page = report['nextPageToken']
            else:
                break
        file_data.close()