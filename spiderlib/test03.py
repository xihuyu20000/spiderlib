import requests
from lxml import etree
from spiderlib import *

#抓取页面内容
resp = requests.get("https://www.cnblogs.com")
#解析出html内容
root = etree.HTML(resp.text)

titles = root.xpath("//a[@class='titlelnk']//text()")
links = root.xpath("//a[@class='titlelnk']//@href")

redis = RedisRedup()

values = []
values.append(['title', 'link'])
for item in zip(titles, links):
    if not redis.loaded(item[1]):
        values.append(item)


# file = FilePipeline()
# file.save(values, file_path="/tmp/aaa.txt")

#指定数据库的ip、port、name、pwd、table_name
mysql = MySQLPipeline(database="bee", table="data")
mysql.save(values)
