from google.cloud import bigquery
import pandas as pd
from pandas.io import gbq
import numpy as np


PROJECT_ID = 'konic-progress-196909'
projectid = '78997000000'  # http://prntscr.com/k0dus1
DATASET_ID = 'MatchedData'
TABLE = 'All_Orders'
import_action = 'append'  # append , 'replace' will overwrite the table if it exists, 'fail' will not overwrite the table if it exists.

client = bigquery.Client(project=PROJECT_ID)
dataset = client.dataset(DATASET_ID)


sql="SELECT date, count(distinct sessionid) as session_all FROM `konic-progress-196909.ukt.owoxbi_sessions_201906*`, unnest(hits) as hits group by 1"


sql_2="with  b as (" \
      "SELECT  date, regexp_replace(regexp_replace(regexp_extract(hits.pagePath,'^([^?]+)?'), '/page/[0-9]', ''), '/filter/|:', '-') as cat, count(distinct sessionid) as session_cat   FROM `konic-progress-196909.ukt.owoxbi_sessions_201906*` , unnest(hits) as hits " \
      "where regexp_contains (hits.pagePath, '^/cat') " \
      "group by 1,2), c as ( " \
      "SELECT date,regexp_replace(regexp_replace(regexp_extract(hits.pagePath,'^([^?]+)?'), '/page/[0-9]', ''), '/filter/|:', '-') as cat, count(distinct  hp.productSku)  as addcart FROM `konic-progress-196909.ukt.owoxbi_sessions_201906*` as t, unnest(hits) as hits, unnest(hits.product) as hp " \
      "where regexp_contains (hits.pagePath, '^/cat') and hits.eCommerceActionType='add' " \
      "group by 1,2) select b.date,b.cat,session_cat,addcart from b  " \
      "left join c on (c.date=b.date and c.cat=b.cat) order by 1 ,3 desc"
def dataFromGBQ():

    df = gbq.read_gbq(sql, projectid,dialect='standard')  # отправка данных в GBQ
    df1 = gbq.read_gbq(sql_2, projectid, dialect='standard')
    df2 = pd.merge(df1, df, on=['date'])
    df2.addcart.fillna(0, inplace=True)
    df2['addcart'] = df2['addcart'].apply(np.int64)
    print(df2.head(20), df2.info())
    df2.to_csv('zoya.csv', sep=';', index=False)


def main():
    # dataToGBQ()
    dataFromGBQ()


if __name__ == '__main__':
    main()
