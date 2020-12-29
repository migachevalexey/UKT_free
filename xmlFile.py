from xml.etree import  ElementTree
import requests

tree=ElementTree.parse(r'C:\Python\ukt\txtcsvFile\catalog.yml.xml')
root=tree.getroot()
z=[]
z1=[]
with open('new.txt', 'r') as f:
    for line in f:
        z.append(line.strip())
with open('barcodeUrlPicture.txt', 'w') as file:
    for offers in root.iter('offers'):
        for offer in offers.findall('offer'):
            try:
                barcode = offer.find('barcode').text
                url_picture = offer.find('picture').text
                # description = offer.find('description').text
                if barcode in z:
                    file.write(f'{barcode} {url_picture}\n')
                    z1.append([barcode,url_picture])
            except: continue


for i in z1:
    r = requests.get(i[1])
    with open(f'picture/{i[0]}.jpg', 'wb') as file_picture:
        file_picture.write(r.content)