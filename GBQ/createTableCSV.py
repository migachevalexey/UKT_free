import pandas as pd
from pandas_gbq import gbq
from tqdm import tqdm
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="G:\Pkeys\keys\glowing-cargo-Yam.json"
projectid = '1067003000000'
import_action = 'append'  # append , 'replace' will overwrite the table if it exists, 'fail' will not overwrite the table if it exists.


df = pd.read_csv(rf'G:\Python\WB\example.csv',  sep=',') #skiprows=1 - убираем заголовок

table_schema = [    {'name': 'SKU', 'type': 'String'},
                    # {'name': 'id', 'type': 'string'},]
                    # {'name': 'Orders', 'type': 'STRING'},]
#                     {'name': 'Date', 'type': 'DATE'},
#                     {'name': 'Total_rub', 'type': 'FLOAT64'},
#                     {'name': 'order_source', 'type': 'STRING'},
#                     {'name': 'sourceMedium', 'type': 'STRING'},
#                     {'name': 'time', 'type': 'STRING'}
                    ]

# ,table_schema=table_schema
gbq.to_gbq(df, f'Temp.example', projectid, if_exists=import_action)
