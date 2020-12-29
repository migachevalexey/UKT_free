import pandas as pd
from pandas.io import gbq
from sqlalchemy import types, create_engine

def dataFromBQ():
    sql_and = fr"select *  FROM `Mig_Data.Orders` WHERE date between '2019-12-21' and '2019-12-21'"
    df_ios = gbq.read_gbq(sql_and, projectid, dialect='standard')
    df_ios['date']=df_ios['date'].apply(pd.to_datetime)
    if 'date' in df_ios.columns: print(True)

    return df_ios


def dataToOracle(conn):
    dataBQ = dataFromBQ()

    dtyp = {c: types.VARCHAR(dataBQ[c].astype(str).str.len().max())
            for c in dataBQ.columns[dataBQ.dtypes == 'object'].tolist()}


    dataBQ.to_sql('orders_channel', conn, if_exists='append', index=False, dtype=dtyp)


oracle_conn =create_engine('oracle+cx_oracle://DWH_:pass@srv-dwh-prod.h.ru/?service_name=DWH?encoding=utf8')

from datetime import datetime

def renamePartition(schema, partParm, suffix, oracle_conn,*tables):
    for table in tables:
        part_name_request = f"""SELECT  TABLE_NAME, PARTITION_NAME
                                FROM ALL_TAB_PARTITIONS
                                WHERE TABLE_NAME = '{table}' and regexp_like( PARTITION_NAME , '{suffix}')"""
        for part_name in oracle_conn.execute(part_name_request):
            for data in oracle_conn.execute(f"""SELECT "{partParm}" FROM {schema}.{table} PARTITION ({part_name[1]}) FETCH FIRST 1 ROWS ONLY"""):
                if isinstance(data[0], datetime):
                    newParam = data[0].strftime("%Y-%m-%d")
                else:  newParam=data[0]
                oracle_conn.execute(f"""ALTER TABLE {schema}.{table} RENAME PARTITION "{part_name[1]}" TO "{newParam}" """)
                print(f"Renamed in {table}: {part_name[1]} -> {newParam}")


# dataToOracle(conn)
renamePartition('DWH', 'date', 'SYS_P', oracle_conn, 'OWOX_SESSION_ID', 'OWOX_HITS_DEMENSION', 'OWOX_HITS','OWOX_SESSION_HITS')