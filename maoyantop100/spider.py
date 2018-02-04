from hashlib import md5
from urllib.parse import urlencode
from multiprocessing import Pool
import os
from pyquery import PyQuery as pq
import requests
import re
import pymongo
from config import *

client = pymongo.MongoClient(MONGO_URL, connect=False)
db = client[MONGO_DB]


def get_page_index(offset):
    print("get page index")
    data = {
        "offset":offset
    }
    param = urlencode(data)
    url = 'http://maoyan.com/board/4'+'?'+param
    try:
        response = requests.get(url)
        if response.status_code == 200:
            #print(response.text)
            return response.text
        return None
    except ConnectionError:
        return None

def parse_page_index(text):
    print("parse page index")
    html = pq(text)
    film_info = html('.container .content .wrapper .main .board-wrapper')
    #print(film_info)
    #compi = '<i.*?board-index.*?">(\d+)</i>.*?<a.*?href.*?title="(.*?)".*?class="star">(.*?)</p>.*?releasetime">(.*?)</p>.*?integer">(.*?)</i>.*?fraction">(.*?)</i>.*?'
    #compi = '<i.*?board-index.*?">(\d+)</i>.*?<a.*?href.*?title="(.*?)".*?class="star">(.*?)</p>.*?releasetime">(.*?)</p>.*?integer">(.*?)</i>.*?fraction">(.*?)</i>.*?'
    #compi = '<i.*?board-index.*?">(\d+)</i>.*?<img.*img.*?src="(.*?)".*?<a.*?href.*?title="(.*?)".*?class="star">(.*?)</p>.*?releasetime">(.*?)</p>.*?integer">(.*?)</i>.*?fraction">(.*?)</i>.*?'
    compi = '<i.*?board-index.*?">(\d+)</i>.*?data-src="(.*?)".*?<a.*?href.*?title="(.*?)".*?class="star">(.*?)</p>.*?releasetime">(.*?)</p>.*?integer">(.*?)</i>.*?fraction">(.*?)</i>.*?'
    items = re.findall(compi,str(film_info),re.S)
    print(items)
    return items

def save_img(content):
    file_path = '{}\{}.{}'.format(os.getcwd(), md5(content).hexdigest(), 'jpg')
    print(file_path)
    if os.path.exists(file_path):
        print(file_path)
        with open(file_path ,"wb") as f:
            f.write(content)
            f.close()

def save_image(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            save_img(response.content)
        return None
    except ConnectionError:
        return False

def parse_page_detail(item):
    print("parse_page_detail")

    data = {
        'index': item[0],
        'title':item[2],
        'star':item[3].strip()[3:],
        'releasetime':item[4][5:],
        'score':item[5]+item[6],
        'url':item[1]
    }
    save_image(data['url'])
    print(data)
    return data

def save_data(data):
    if db[MONGO_TABLE].insert(data):
        print('insert data successful',data)
        return True
    return False

def main(offset):
    text = get_page_index(offset)
    items =  parse_page_index(text)
    for item in items:
        result = parse_page_detail(item)
        if result:
            save_data(result)

if __name__ == "__main__":
    pool = Pool()
    groups = [ 10*i for i in range(GROUP_START,GROUP_END+1)]
    pool.map(main,groups)
    pool.close()
    pool.join()