import json
from typing import Optional
import base64

import feedparser
import pymysql
import requests
import redis


class RedisRedup:
    """
    redis判断重复
    """
    NAME = 'spider_urls'

    def __init__(self, host:str='127.0.0.1', port:int=6379, db:int=0, password:Optional[str]=None):
        """
        初始化
        :param host: ip或者hostname
        :param port: 端口号
        :param db: 数据库
        :param password: 密码
        """
        self.pool = redis.ConnectionPool(host=host, port=port, db=db, password=password)
        self.r = redis.Redis(connection_pool=self.pool)

    def isRedup(self, url, name=NAME):
        """
        判断是否重复
        :param url:
        :param name:
        :return:
        """
        if self.r.sismember(name, url):
            return True
        return False

    def add(self, url, name=NAME):
        """
        加入
        :param url:
        :param name:
        :return:
        """
        print("新增去重 {url}".format(url=url))
        self.r.sadd(RedisRedup.NAME, url)

    def __str__(self):
        return 'RedisRedup'


class RssParser:
    """
    解析rss
    """
    def parse(self, url)->list:
        feed = feedparser.parse(url)
        return feed.get("entries")


class FilePipeline:
    """
    结果输出到文件
    """
    def __init__(self, path: str = "data.txt", sep: str = "\t", linesep: str = "\r\n"):
        """
        保存到文件
        :param path: 文件路径
        :param sep:  列之间的分隔符
        :param linesep: 行之间的分隔符
        """
        self.path = path
        self.sep = sep
        self.linesep = linesep

    def save(self, values, file_path: Optional[str]):
        """
        保存。有错抛异常
        :param values: values是个list嵌套list。里面的第1个list是标题，后面的list都是数据
        :param file_path: 自定义保存路径
        :return:
        """
        path = file_path if file_path else self.path
        with open(path, 'a', encoding='utf8') as f:
            for line in values:
                f.write(self.sep.join(line))
                f.write(self.linesep)
                f.flush()

    def __str__(self):
        return 'FilePipeline'


class MySQLPipeline:
    """
    结果输出到MySQL
    """
    def __init__(self, host="localhost", user="root", password="admin", database="test", table="data", port=3306, charset='utf8mb4'):
        """
        初始化
        :param host:    ip或者hostname，不带端口号
        :param user:    用户名
        :param password:    密码
        :param database:    数据库名称
        :param table:   表名称，后面也可以修改
        :param port:    端口
        :param charset:     字符集
        """
        self.db = pymysql.connect(host=host, user=user, password=password, database=database, port=port, charset=charset)
        self.c = self.db.cursor()
        self.table_name = table

    def save(self, values, table_name: str = '')->None:
        """
        保存数据。
        :param values: 是list嵌套list，里面的第一个list是字段名，剩余的list都是数据
        :return: 有错抛异常
        """
        sql = ''
        t_name = table_name if table_name else self.table_name
        try:
            fields = ",".join(values[0])
            for index in range(1, len(values)):
                # 执行sql语句
                sql = u"""INSERT INTO {name}({fields}) VALUES ({values})""".format(name=t_name, fields=fields,  values=",".join(['"'+pymysql.escape_string(v)+'"' for v in values[index]]))
                self.c.execute(sql)
            # 提交到数据库执行
            self.db.commit()
        except Exception as e:
            # 如果发生错误则回滚
            self.db.rollback()
            raise e

    def __str__(self):
        return 'MySQLPipeline'

    def __del__(self):
        if self.c:
            self.c.close()
        if self.db:
            self.db.close()


class WordPressPipeline:
    """
    结果发布到WordPress。
    需要在wp中使用basic authentication
    """
    def __init__(self, host:str='localhost', user:str='root', password:str='admin'):
        self.host = host
        self.user = user
        self.password = password

    def save(self, values, tag: str = '') -> None:
        """
        保存。输入的字段必须是title和content
        :param values: 是list嵌套list，里面的list有2个值，分别是title和content
        :return: 有错抛异常
        """
        for article in values[1:]:
            try:
                data = {
                    'title': article[0],
                    'content': article[1],
                    'status': "publish",
                    'comment_status': "open"
                }
                auth = str.encode('{}:{}'.format(self.user, self.password))
                headers = {'Authorization': 'Basic ' + str(base64.b64encode(auth), 'utf-8')}
                url = "http://{host}/index.php/wp-json/wp/v2/posts".format(host=self.host)
                resp = requests.post(url, headers=headers, data=data, timeout=5)
                if resp.status_code>201:
                    raise Exception(resp)
            except Exception as e:
                raise e

    def __str__(self):
        return 'WordPressPipeline'


class DrupalPipeline:
    """
    结果发布到Drupal8中。
    需要在wp中使用basic authentication
    """
    def __init__(self, host:str='localhost', user:str='root', password:str='admin'):
        self.host = host
        self.user = user
        self.password = password

    def save(self, values)->None:
        """
        发布文章
        :param values:是list嵌套list，里面list的第1个值是标题，第2个值是内容
        :return:
        """
        headers = {'Accept': 'application/vnd.api+json', 'Content-Type': 'application/vnd.api+json', }
        for item in values:
            title = item[0]
            content = item[1]
            data = {
                "data": {
                    "type": "node--article",
                    "attributes": {
                        "title": title,
                        "body": {
                            "value": content,
                            "format": "plain_text"
                        }
                    }
                }
            }
            resp = requests.post("http://{}/jsonapi/node/article".format(self.host), auth=(self.user, self.password), headers=headers,
                                 data=json.dumps(data))
