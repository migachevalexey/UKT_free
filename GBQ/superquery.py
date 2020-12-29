import pymysql.cursors

#https://web.superquery.io/python/
#https://github.com/superquery/superPy

connection = pymysql.connect(host='bi.superquery.io',
                             user='Sm',
                             password='rkl',
                             db='konic-progress-196909.ukt',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
print("connection successful!")
try:

    with connection.cursor() as cursor:

        # SQL
        sql = "SELECT * FROM `konic-progress-196909.Mig_Data.Trafic`"

        # Execute query.
        cursor.execute(sql)

        print("cursor.rowcount: ", cursor.rowcount)
        for row in cursor:
            print(row)
        # SQL
        # sql = "explain;"
        #
        # # Execute query.
        # cursor.execute(sql)
        #
        # print("cursor.description: ", cursor.fetchall())
        # print()
        # for row in cursor:
        #     print(row)

finally:
    # Close connection.
    connection.close()