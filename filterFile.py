import re
import pyperclip

def filefilter():
    with open('09-18.txt', 'r') as f, open('09-18qt0.txt', 'w') as wr:
        for line in f:
            if '&qt=0&'not in line:
                wr.write(line)
                # wr.write(re.search(r'09[0-9]{10}', line).group(0) + '\n')

a='''3392423
3382949
3371892
3387123
3391194
3394665
3393157
3394734
3373691
3355012'''.replace('\n', '|').strip()


pyperclip.copy(a)
print(a)
