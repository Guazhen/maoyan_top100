import json
from multiprocessing.pool import Pool
from urllib.parse import urlencode
import pymongo
import re

import os
import requests
from bs4 import BeautifulSoup
from hashlib import md5
from config import *

client = pymongo.MongoClient(MONGO_URL,connect=False)
db = client[MONGO_DB1]

def get_page_index(keyword,offset):
    data = {
    'offset': offset,
    'format': 'json',
    'keyword': keyword,
    'autoload': 'true',
    'count': 20,
    'cur_tab': 1,
    'from':'search_tab'
    }
    param = urlencode(data)
    url = 'https://www.toutiao.com/search_content/' + '?' + param
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print(response.text)
            return response.text
        return None
    except ConnectionError:
        return None

def parse_page_index(response):
    data = json.loads(response)
    print(data.keys())
    if data and 'data' in data.keys():
        for item in data.get('data'):
            yield item.get('display_url')

def get_page_detail(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            # print(response.text)
            return response.text
        return None
    except ConnectionError:
        return None

def download_image(image):
    try:
        response = requests.get(image)
        if response.status_code == 200:
            # print(response.text)
            save_image(response.content)
            #return response.content
        return None
    except ConnectionError:
        return None

def save_image(content):
    filename = '{}/{}.{}'.format(os.getcwd(), md5(content).hexdigest(),'jpg')
    with open(filename,'wb') as f:
        f.write(content)
        f.close()

def save_mongo(data):
    if db[MONGO_TABLE1].insert(data):
        print("insert into table",data)
        return True
    return None

def parse_page_detail(html,url):
    soup = BeautifulSoup(html, 'lxml')
    result = soup.select('title')
    title = result[0].get_text() if result else ' '
    print(title)
    print("parse page detail")
    pattern = '<html.*?gallery: JSON.parse\("(.*?)"\)'
    results = re.findall(pattern,html,re.S)
    if len(results):
        data = results[0]
        item = data.replace('\\','')
        detail_data = json.loads(item)
        if detail_data and 'sub_images' in detail_data.keys():
            urls = detail_data.get('sub_images')
            # print(urls)
            for sub_url in urls:
                suburl1 = sub_url.get('url')
                download_image(suburl1)
                yield {
                    'url':url,
                    'title':title,
                    'image':suburl1
                }
def main(offset):
    text = get_page_index(KEY_WORD,offset)
    for url in parse_page_index(text):
        if url:
            text = get_page_detail(url)
            items = parse_page_detail(text,url)
            # print(items)
            for item in items:
                save_mongo(item)
        else:
            pass

if __name__ == "__main__":
    pool = Pool()
    groups = ([ i*20 for i in range(GROUP_START,GROUP_END+1)])
    pool.map(main,groups)
    pool.close()
    pool.join()
