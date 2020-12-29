import MySQLdb
import json
import itertools

"Из URL сайта тянем название используемого фильтра из MySQL DB" \

file_db_connect = 'C:/Python/ukt/pass/MySQL_db_connect.json'
with open(file_db_connect) as f:
    param_сonnect = json.load(f)

open(r"C:\Python\ukt\txtcsvFile\filterCatName.txt", "w").close() # очищаем файл

db_connect = MySQLdb.connect(user=param_сonnect['user'], passwd=param_сonnect['passwd'],
                             host=param_сonnect['host'], db=param_сonnect['db_prod'], charset='cp1251')
cursor = db_connect.cursor()

with open(r'C:\Python\ukt\txtcsvFile\filterCatID.txt') as catId, \
     open(r'C:\Python\ukt\txtcsvFile\filterCatName.txt', 'a', encoding='cp1251') as catName:
    for i in catId:
        p = []
        s = i.replace('/cat/', '').split('-')
        for i in [f'select name from  CATALOGUE  where  id = {s[0]}',
                  f'select title from  ukt_prop  where  id = {s[1]}',
                  f'select value from ukt_prop_value where id={s[2]}']:
            cursor.execute(i)
            sql_data = cursor.fetchall()  # r=cursor.fetchmany(5)
            p += list(sql_data)
        merged = list(itertools.chain(*p))
        catName.write('->'.join(merged) + '\n')

cursor.close()
db_connect.close()