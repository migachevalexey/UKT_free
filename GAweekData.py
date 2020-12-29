from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials as SAC
import httplib2
import pandas as pd
import os

'''
Тянем двнные из GA сразу по сайту и APP и записываем данные в 1 файл.
Можно сразу через готовый отчет тянуть из GA
https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/unsampledReports/get
'''

profil_id = ['54989000', '101244000', '100729000']  # kor.ru(Реальные данные), Android, iOS
Files = ['WEB', 'Android', 'iOS']


def get_service(api_name, api_version, scope, key_file_location,
                service_account_email):
    credentials = SAC.from_p12_keyfile(
        service_account_email, key_file_location, scopes=scope)
    http = credentials.authorize(httplib2.Http())
    service = build(api_name, api_version, http=http)

    return service


def get_results(service, prof_id):
    if prof_id == '54989000':
        m2 = 'ga:transactions'
    else:
        m2 = 'ga:newUsers'

    z = service.data().ga().get(
        ids='ga:' + prof_id,
        start_date='7daysAgo',
        end_date='yesterday',
        metrics=f'ga:sessions,{m2},ga:transactionRevenue',
        dimensions="ga:date",
        sort='ga:date',
        max_results='10000'
    ).execute()
    return z['rows']


def main():
    scope = ['https://www.googleapis.com/auth/analytics.readonly']
    service_account_email = 'kor@coherent-acre-182000.iam.gserviceaccount.com'
    key_file_location = 'C:/Python/kor/kor-a5472b08df12.p12'
    service = get_service('analytics', 'v3', scope, key_file_location, service_account_email)
    for j, i in enumerate(profil_id):
        api_data = get_results(service, i)
        df = pd.DataFrame(api_data)
        df.to_csv('C:/Python/d_week_GA_Params.csv', sep=';', header=['date', Files[j], Files[j], Files[j]],
                  mode='a', index=None)


if __name__ == '__main__':
    main()
