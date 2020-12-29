# отправка товаров(цена, количество) в транзакции
from builtins import enumerate

y = [['09855555', '111', '123.123', 222, 33]] # пример: транзакция
z = {'09855555': {123: [1, 21.0], 321: [2, 33.0]}} # пример: товары в транзакции
x = {}
for i in y:
    payload = {'v': '1',
               'tid': 'UA-8149186-8',
               't': 'pageview',
               'cid': i[2],
               'ti': i[0],
               'dp': '/ordering/thanks',
               'ta': 'CallCenter',
               'tr': i[3],  # Revenue.
               'ts': i[4],  # Shipping.
               'pa': 'purchase',
               'uid': i[1],
               'qt': 12600000,  # 3.5 часа в милисекндах
               'uip': '1.1.1.1',
               'ni': 1
               }
    s = z[i[0]]
    # тут формируем dict c товарами
    for i, (key, val) in enumerate(s.items(), start=1):
        x.update({f'pr{i}id': key,
                  f'pr{i}pr': val[0],
                  f'pr{i}qt': val[1]})
    payload.update(x)
print(payload)