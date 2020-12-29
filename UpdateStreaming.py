import datetime
from collections import defaultdict
from google.cloud import bigquery

PROJECT_ID = 'konic-progress-196909'
DATASET_ID = 'ukt'
client = bigquery.Client(project=PROJECT_ID)
dataset = client.dataset(DATASET_ID)

'''
Данный скрипт обновляет поле page.pagePath через регулярку по выбранным датам и нужным hitId в хитовом стриминге данных OWOX' \
Важнное замечание: в SQL запросе в in передаем список(list) hitid и что бы GBQ его понял делаем списку UNNEST
hitid.txt - предаврительно выгружаем из GBQ. Превращаем его в dict.
Здесь же удаляем из сесионного стриминга выбранные хиты(файл sessionId.txt). удаление происходит через update ARRAY
Добаил update сессионного стриминга - def updateSessionHit()
'''

d = defaultdict(list)

currDate = (datetime.date.today() - datetime.timedelta(days=2)).strftime("%Y%m%d")


def hitIdDict():
    QUERY = "SELECT date, hitid FROM `ukt.streaming_{}` where regexp_contains(page.pagePath , 'password=.+')".format(
        currDate)
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    for row in rows:
        key, val = row.date, row.hitid  # или row[0], row[1]  или row['date'], row['hitid']
        d[key].append(val)

    # with open("c:/Python/ukt/hitId.txt") as f:
    #     for line in f:
    #         key, val = line.split()
    #         d[key].append(val)
    return d


def sessionIdDict():
    # QUERY ='SELECT date,  sessionId , hits.hitid FROM `ukt.owoxbi_sessions_2018*`, UNNEST(hits) as hits where regexp_contains(hits.page.pagePath , "password=.+")'
    QUERY =""" select date, sessionId , hits.hitid
  from `ukt.owoxbi_sessions_2019*`, unnest(hits) as hits  where hits.eCommerceAction.action_type ='purchase'
  and hits.eventInfo.eventCategory='Interactions'
  and hits.eventInfo.eventAction ='click_to_catalog' and _TABLE_SUFFIX between '0715' and '1013'"""

    c = defaultdict(dict)
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    for row in rows:
        key, s, h = row[0], row[1], row[2]
        d[s].append(h)
        c[key.replace('-', '')][s] = d[s]

    # with open("c:/Python/ukt/Cron/sessionId.txt") as f:
    #     for line in f:
    #         key, s, h = line.split()
    #         d[s].append(h)
    #         c[key][s] = d[s]

    return c


def upd_data(QUERY):
    query_job = client.query(QUERY)
    z = query_job.result()
    print(query_job.state, query_job.num_dml_affected_rows, "rows affected")
    return z


def updateHit(dictList):
    for dt, hitID in dictList.items():
        QUERY_update = "update `{0}.{1}.streaming_{2}` set page.pagePath = regexp_replace(page.pagePath, 'login.*', 'login=') " \
                       " where hitId in UNNEST({3})".format(PROJECT_ID, DATASET_ID, dt, hitID)
        upd_data(QUERY_update)



def changeSessionHit(dictList, QUERY):
    for dt, sesIdHitId in dictList.items():
        for sesID, hitID in sesIdHitId.items():
            upd_data(QUERY.format(PROJECT_ID, DATASET_ID, dt, hitID, sesID))


# Этим запросом делаем update nested fields 3 level в сессионном стриминге
SQL_upd_SessHit = 'update `{}.{}.owoxbi_sessions_{}` set hits = array(select as struct * replace((select as struct page.* replace(regexp_replace( page.pagePath , "login=.*", "login") as pagePath)) as page) ' \
                  'from unnest(hits) ' \
                  'where hitid  in Unnest({})) where  sessionId="{}"'

# Этим запросом делаем update nested fields 2 level в сессионном стриминге
'UPDATE  `{}.{}.owoxbi_sessions_{}` SET hits = ARRAY(select as struct * replace(REGEXP_REPLACE( pagePath , r"login=.*", "login") as  pagePath) ' \
'FROM UNNEST(hits) WHERE hitid IN UNNEST({})) ' \
'WHERE sessionId="{}"'

# На крайний случай. Выпиливаем весь хит из сессии
SQL_del_SessHit = 'update `{}.{}.owoxbi_sessions_{}` set hits = ARRAY( SELECT h FROM UNNEST(hits) AS h where h.hitid not in UNNEST({})) ' \
                  'where sessionID = "{}"'

def main():
    # data = hitIdDict()
    # updateHit(data)


    data = sessionIdDict()
    changeSessionHit(data, SQL_del_SessHit)

if __name__ == '__main__':
    main()
