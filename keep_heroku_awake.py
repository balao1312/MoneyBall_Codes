import requests
import time

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/80.0.3987.132 Safari/537.36'}
while 1:
    res = requests.get('https://oolala.herokuapp.com', headers=headers)
    print(res.text)
    time.sleep(60)
