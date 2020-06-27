#!/usr/bin/env python
# coding: utf-8
from elasticsearch import Elasticsearch
import requests, json, pandas
from datetime import datetime

# 測試連線是否正常
url = 'http://10.120.35.108:9200'
print(requests.get(url), ' ======>    Server Connected')

# 建立連線
es = Elasticsearch('http://10.120.35.108:9200')
print('ES object : ', es)

# 建立一個 index
es.indices.create(index='test-index', ignore=400)

# 刪除一個 index
# es.indices.delete(index='test-index', ignore=400)

# 搜尋 & 用 json 去 dump 出來
data = es.search(index='twinttweets', size=100)
jj = json.dumps(data, indent=2)
print('jj : \n', jj)

print(type(data))

tweet_data = []

for ii in data:
    print(ii)  # took, timed_out, _sahrds, hits
print(len(data['hits']['hits']))
for ii, content in enumerate(data['hits']['hits']):
    try:
        print(f'{"=" * 50} \n{ii} : \n{"-" * 50}')
        print(content["_source"]["name"])
        print(content["_source"]["id"])
        print(content["_source"]["date"])
        print(content["_source"]["link"])
        print(content["_source"]["nlikes"])
        print(content["_source"]["nreplies"])
        print(content["_source"]["nretweets"])
        print(content["_source"]["tweet"])

        tweet_data.append([
            content["_source"]["name"],
            content["_source"]["id"],
            content["_source"]["date"],
            content["_source"]["link"],
            content["_source"]["nlikes"],
            content["_source"]["nreplies"],
            content["_source"]["nretweets"],
            content["_source"]["tweet"],
        ])

    except Exception as e:
        print(e)
        continue
columns = ['name', 'id', 'date', 'link', 'nlikes', 'nreplies', 'nretweets', 'tweet']
df = pandas.DataFrame(tweet_data)
df.columns = columns

print(df)
df.to_csv('ooo.csv', encoding='utf-8-sig')

# 迴圈塞資料
# # doc = {
#     'author': 'Balao',
#     'text': 'oookkoo1oo',
#     'timestamp': datetime.now()
#     }

# for i in range(15):
#     es.create(index='test-index', id =i, body=doc)


# 用 filter path 篩選結果
# data = es.search(index='test', filter_path=['hits.hits._id', 'hits.hits._type'])
# print(data)
# for anyy in data['hits']['hits']:
#     print(anyy)
