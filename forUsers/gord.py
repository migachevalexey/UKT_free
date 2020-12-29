import MySQLdb
import json
import pandas as pd
import timeit

file_db_connect = 'C:/Python/ukt/pass/MySQL_db_connect.json'
with open(file_db_connect) as f:
    param_сonnect = json.load(f)
db_connect = MySQLdb.connect(user=param_сonnect['user'], passwd=param_сonnect['passwd'],
                             host=param_сonnect['host'], db=param_сonnect['db_prod'], charset='cp1251')

items_id = pd.read_sql('select DISTINCT item_id  from  ukt_item_price where item_id>50000 limit 10000', con=db_connect)

df = pd.DataFrame()
for index, row in items_id.iterrows():
    p_old = pd.read_sql(
        f'SELECT item_id, max(start_date) as OLD_start_date, price as OLD_price from ukt_item_price where active=0 and item_id ={row["item_id"]} '
        f'UNION ALL '
        f'select item_id , start_date as NEW_start_date, price as NEW_price from ukt_item_price where active=1 and item_id ={row["item_id"]}',
        con=db_connect)

    p_old.at[0, 'NEW_start_date'] = p_old.iloc[1][1]
    p_old.at[0, 'NEW_price'] = p_old.iloc[1][2]
    p_old.drop([1], inplace=True)
    df = pd.concat([df, p_old])
df = df[['item_id', 'OLD_start_date', 'NEW_start_date', 'OLD_price', 'NEW_price']]
df['ratio'] = df['NEW_price'] / df['OLD_price']
df_filter = df.query('ratio<0.95 | ratio>1.05')
writer = pd.ExcelWriter('gordeev1.xlsx', engine='xlsxwriter')
df.to_excel(writer, index=False, sheet_name='all')
df_filter.to_excel(writer, index=False, sheet_name='filter')
writer.save()



''' Так медленнее на 13%! За счет 2х SQL запросов к базе
start_timer = timeit.default_timer()
df = pd.DataFrame()
for index, row in items_id.iterrows():
    p_old = pd.read_sql(
        f'select max(start_date) as OLD_start_date, price as OLD_price  from ukt_item_price where active=0  and item_id = {row["item_id"]}',
        con=db_connect)
    p_new = pd.read_sql(
        f'select item_id , start_date as NEW_start_date, price as NEW_price from ukt_item_price where active=1  and item_id = {row["item_id"]}',
        con=db_connect)
    final = pd.concat([p_old, p_new], axis=1)
    df = pd.concat([df, final])

df = df[['item_id', 'OLD_start_date', 'NEW_start_date', 'OLD_price', 'NEW_price']]
df['ratio'] = df['NEW_price'] / df['OLD_price']
df_filter = df.query('ratio<0.95 | ratio>1.05')
writer = pd.ExcelWriter('gordeev.xlsx', engine='xlsxwriter')
df.to_excel(writer, index=False, sheet_name='all')
df_filter.to_excel(writer, index=False, sheet_name='filter')
writer.save()
stop_timer = timeit.default_timer()
print("Время генераци строки:", stop_timer - start_timer);
'''