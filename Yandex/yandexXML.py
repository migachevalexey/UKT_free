import requests
from xml.etree import ElementTree

# https://tech.yandex.ru/xml/doc/dg/concepts/get-request-docpage/

word = 'доставка продуктов на дом'

params = {'key': '03.00000:0000', 'user': 'heatmap', 'query': word, 'lr': 1, 'page': 1,
          'l10n': 'ru', 'sortby': 'rlv', 'maxpassages': 1}
zz = requests.get('https://yandex.ru/search/xml', params=params)

with open(f'body.xml', 'wb') as file_picture:
    file_picture.write(zz.content)

tree = ElementTree.parse('body.xml')
root = tree.getroot()

for offers in root.iter('grouping'):
    for offer in offers.findall('group'):
        for off in offer.findall('doc'):
            url = off.find('url').text
            print(url)