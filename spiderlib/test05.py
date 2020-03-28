import json

import requests

#下载文章
# resp = requests.get("http://192.168.1.90:86/jsonapi/node/article")

#发布文章
headers = {'Accept': 'application/vnd.api+json','Content-Type': 'application/vnd.api+json',}
data = {
  "data": {
    "type": "node--article",
    "attributes": {
      "title": "My custom title",
      "body": {
        "value": "Custom value",
        "format": "plain_text"
      }
    }
  }
}
resp = requests.post("http://192.168.1.90:86/jsonapi/node/article", auth=('root','admin'), headers=headers, data=json.dumps(data))
print(resp)
print(resp.text)