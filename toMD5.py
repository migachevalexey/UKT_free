import hashlib
import zipfile
import os
import pandas as pd
# pd.options.display.float_format = "{:,.0f}".format

# тут не используем больше
# def stringToHash(my_string,code):
#     if code == 'md5': m = hashlib.md5()
#     else: m = hashlib.sha256()
#     m.update(my_string.encode('utf-8'))
#     return m.hexdigest()

#https://stackoverflow.com/questions/24220860/passing-an-attribute-name-to-a-function-in-python

def fileToHashPandas(code, data):
    # пеередаем атрибут(code) метода(hashlib) в параметре функици вытягиваем его через getattr(hashlib,code)
    sourceDir = r'c:/Python/hash/'
    sourceFile = sourceDir + data + '.xlsx'
    df = pd.read_excel(sourceFile)
    df_phone = df['phone'].drop_duplicates().dropna().replace('[^\d.]+', '', regex=True).apply(str)
    df_mail = df['mail'].drop_duplicates().dropna()
    # df_phone_md5 = df_phone.apply(lambda x: stringToHash(x, code))
    # df_mail_md5 = df_mail.apply(lambda x: stringToHash(x, code))
    df_phone_md5 = df_phone.apply(lambda x: getattr(hashlib,code)(x.encode('utf-8')).hexdigest())
    df_mail_md5 = df_mail.apply(lambda x: getattr(hashlib,code)(x.encode('utf-8').strip().lower()).hexdigest())
    df_mail_md5.to_csv(sourceDir + f'{code}_{data}_mail.csv', index=False, header=False)
    df_phone_md5.to_csv(sourceDir + f'{code}_{data}_phone.csv', index=False, header=False)

    with zipfile.ZipFile(sourceDir+f'{code}_{data}_mail' + '.zip', 'w') as myzipMail, \
            zipfile.ZipFile(sourceDir+f'{code}_{data}_phone' + '.zip', 'w') as myzipPhone:
        myzipMail.write(sourceDir+f'{code}_{data}_mail.csv',
                        compress_type=zipfile.ZIP_BZIP2)  # степень сжатия - DEFLATED  BZIP2  LZMA
        myzipPhone.write(sourceDir+f'{code}_{data}_phone.csv', compress_type=zipfile.ZIP_BZIP2)


fileToHashPandas('md5', 'клиенты не делавшие заказы пол года v1')
#sha256
