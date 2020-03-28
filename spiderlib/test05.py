import requests
from lxml import html
from spiderlib import DrupalPipeline, RedisRedup

resp = requests.get("http://www.ala.org/news/press-releases")
if resp.status_code==200:
  links = html.etree.HTML(resp.text).xpath("//span[@class='field-content']//@href")
  links = ["http://www.ala.org"+x for x in links]

  redis = RedisRedup()
  d8 = DrupalPipeline(host="192.250.197.186:8887", user="developer", password="webadmin-password-123")
  for link in links:
    if redis.isRedup(link):
      continue
    resp = requests.get(link)
    if resp.status_code==200:
      root = html.etree.HTML(resp.text)
      title = root.xpath("//h1[@class='page-header']//text()")
      title = "".join(title)
      content = root.xpath("//div[@class='field-items']//text()")
      content = "\r\n".join(content)
      content = "来源地址 "+link+"\r\n"+content
      d8.save_one(title, content)
      redis.add(link)
