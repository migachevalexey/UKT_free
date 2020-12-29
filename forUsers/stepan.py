import pandas as pd
from pandas.io import gbq

projectid = '78997000000'

sql =  "with b as " \
      "(SELECT  regexp_replace( regexp_replace(regexp_replace(regexp_extract(hits.pagePath,'^([^?]+)?'), '/page/[0-9].*', ''), '-.*|/filter.*|/catalogue_filter.*', ''),'/catalogue/','/') as cat, count(distinct sessionid) as showCatalog " \
      "FROM `ukt.owoxbi_sessions_*`, unnest(hits) as hits " \
      "where _TABLE_SUFFIX between  FORMAT_DATE('%Y%m%d', DATE_ADD(current_date(), INTERVAL -{} DAY)) and FORMAT_DATE('%Y%m%d', DATE_ADD(current_date(), INTERVAL -{} DAY)) and regexp_contains (hits.pagePath, '^/cat') " \
      "group by 1)," \
      "c as (SELECT  regexp_replace( regexp_replace(regexp_replace(regexp_extract(hits.pagePath,'^([^?]+)?'), '/page/[0-9].*', ''), '-.*|/filter.*|/catalogue_filter.*', ''),'/catalogue/','/') as cat, count(hits.hitid)  as addCartSKU " \
      "FROM `konic-progress-196909.ukt.owoxbi_sessions_*` as t, unnest(hits) as hits " \
      "where _TABLE_SUFFIX between  FORMAT_DATE('%Y%m%d', DATE_ADD(current_date(), INTERVAL -{} DAY)) and FORMAT_DATE('%Y%m%d', DATE_ADD(current_date(), INTERVAL -{} DAY)) and regexp_contains (hits.pagePath, '^/cat') and hits.eCommerceActionType='add' and hits.eCommerceAction.list in ('Catalog','cat_item_news', 'cat_item_personal','CatalogHaveTime') " \
      "group by 1)," \
      "a as (SELECT  regexp_replace( regexp_replace(regexp_replace(regexp_extract(hits.pagePath,'^([^?]+)?'), '/page/[0-9].*', ''), '-.*|/filter.*|/catalogue_filter.*', ''),'/catalogue/','/') as cat, count(distinct  clientid)  as addCartClient " \
      "FROM `konic-progress-196909.ukt.owoxbi_sessions_*` as t, unnest(hits) as hits " \
      "where _TABLE_SUFFIX between  FORMAT_DATE('%Y%m%d', DATE_ADD(current_date(), INTERVAL -{} DAY)) and FORMAT_DATE('%Y%m%d', DATE_ADD(current_date(), INTERVAL -{} DAY)) and regexp_contains (hits.pagePath, '^/cat') and hits.eCommerceActionType='add' and hits.eCommerceAction.list in ('Catalog','cat_item_news', 'cat_item_personal','CatalogHaveTime') " \
      "group by 1) " \
      "select regexp_replace(b.cat,'/cat/','') as id,uktc.name,showCatalog,addCartSKU,addcartClient, round(100*addcartClient/showCatalog,4) as CR  " \
      "from b " \
      "left join c on (c.cat=b.cat) " \
      "left join a on (a.cat=b.cat) " \
      "left join `Mig_Data.uktCatalog` uktc on (safe_cast(regexp_replace(b.cat,'/cat/','') as int64) =uktc.id) " \
      "order by 3 desc"


def dataFromGBQ():
    df = gbq.read_gbq(sql.format(*[14,1]*3), projectid, dialect='standard')  # *[14,1]*3 - первая * - разлагаем список в элементы, *3 - умножаем список на 3
    df1 = gbq.read_gbq(sql.format(*[28,15]*3), projectid, dialect='standard')
    df1.drop('name', axis=1,inplace=True)
    df2 = pd.merge(df,df1, on=['id'])
    print(df2.head())
    df2 = df2[~df2.name.isin([None])]
    df2.iloc[:500].to_excel('zoya1.xlsx', index=False)


def main():
    dataFromGBQ()


if __name__ == '__main__':
    main()
