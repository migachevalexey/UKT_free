from pyrfc import Connection, ABAPApplicationError, ABAPRuntimeError, LogonError, CommunicationError
conn = Connection(ashost='10.50.x.x', sysnr='01', client='740', user='SAS', passwd='pass')
result = conn.call('STFC_CONNECTION', REQUTEXT=u'Hello SAP!')
print(result)

# http://www.alexbaker.me/code/python-and-sap-part-1-connecting-to-sap

def get_table_info(**kwargs):
    try:
        host = kwargs["host"]
        sysnr = kwargs["sysnr"]
        client = kwargs["client"]
        user = kwargs["user"]
        pwd = kwargs["pass"]
        table_name = kwargs["table"]
        conn = Connection(ashost=host, sysnr=sysnr, client=client, user=user, passwd=pwd)
        options = [{'TEXT': "TABNAME = '{}'".format(table_name)}]
        fields = [
                {'FIELDNAME': 'TABNAME'},
                {'FIELDNAME': 'FIELDNAME'},
                {'FIELDNAME': 'POSITION'},
                {'FIELDNAME': 'DDTEXT'}
                ]
        rows_at_a_time = 100
        row_skips = 0
        print('| TABNAME | FIELDNAME |  POSITION| DDTEXT |')
        print('| --- | --- | --- | --- |')
        while True:
            result = conn.call('RFC_READ_TABLE',
                               QUERY_TABLE='DD03VT',
                               OPTIONS=options,
                               FIELDS=fields,
                               ROWSKIPS=row_skips,
                               ROWCOUNT=rows_at_a_time,
                               DELIMITER='|')
            for row in result['DATA']:
                print('| '+' | '.join([r.strip() for r in row['WA'].split('|')]) + ' |')

            row_skips += rows_at_a_time
            if len(result['DATA']) < rows_at_a_time:
                break

    except CommunicationError:
        print(u"Could not connect to server.")
        raise
    except LogonError:
        print(u"Could not log in. Wrong credentials?")
        raise
    except (ABAPApplicationError, ABAPRuntimeError):
        print(u"An error occurred.")
        raise
		
params ={
    "host": "10.50.x.x",
    "sysnr": "01",
    "client": "740",
    "user":"SAS",
    "pass": "pass",
    "table": "/SAPAPO/VERSKEY"
  }
  

import re

class main():
    def __init__(self):
        ASHOST='10.50.x.x'
        CLIENT='740'
        SYSNR='01'
        USER='SAS'
        PASSWD='pass'
        self.conn = Connection(ashost=ASHOST, sysnr=SYSNR, client=CLIENT, user=USER, passwd=PASSWD)


    def qry(self, Fields, SQLTable, Where = '', MaxRows=50, FromRow=0):
        """A function to query SAP with RFC_READ_TABLE"""

        # By default, if you send a blank value for fields, you get all of them
        # Therefore, we add a select all option, to better mimic SQL.
        if Fields[0] == '*':
            Fields = ''
        else:
            Fields = [{'FIELDNAME':x} for x in Fields] # Notice the format

        # the WHERE part of the query is called "options"
        options = [{'TEXT': x} for x in Where] # again, notice the format

        # we set a maximum number of rows to return, because it's easy to do and
        # greatly speeds up testing queries.
        rowcount = MaxRows

        # Here is the call to SAP's RFC_READ_TABLE
        tables = self.conn.call("BBP_RFC_READ_TABLE", QUERY_TABLE=SQLTable, DELIMITER='|', FIELDS = Fields, OPTIONS=options, ROWCOUNT = MaxRows, ROWSKIPS=FromRow)

        # We split out fields and fields_name to hold the data and the column names
        fields = []
        fields_name = []

        data_fields = tables["DATA"] # pull the data part of the result set
        data_names = tables["FIELDS"] # pull the field name part of the result set

        headers = [x['FIELDNAME'] for x in data_names] # headers extraction
        long_fields = len(data_fields) # data extraction
        long_names = len(data_names) # full headers extraction if you want it

        # now parse the data fields into a list
        for line in range(0, long_fields):
            fields.append(data_fields[line]["WA"].strip())

        # for each line, split the list by the '|' separator
        fields = [x.strip().split('|') for x in fields ]

        # return the 2D list and the headers
        return fields, headers
 

s = main() 

# Choose your fields and table
fields = ['MATNR', 'EAN11']
table = 'MEAN'
# you need to put a where condition in there... could be anything
where = ['MATNR <> 0']

# max number of rows to return
maxrows = 10

# starting row to return
fromrow = 0

# query SAP
results, headers = s.qry(fields, table, where, maxrows, fromrow)

print(headers)
print(results)
