import platform
import signal

name = 'spiderlib'
author = '吴超  QQ 377486624'

import subprocess
import time
import traceback
import os
import redis
import asyncio
from pyppeteer import launch
from lxml import html
import pymysql
import numpy as np
import base64
import requests


class NoLogger:
    """
    不输出日志
    """
    def log(self, tag, info, ts=0):
        pass


class ConsoleLogger:
    """
    输出日志到控制台
    """
    def log(self, tag, info, ts=0):
        cur = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        if ts:
            print("{}\t{}\t{}\t耗时{}秒".format(cur, tag, info, round(ts, 3)))
        else:
            print("{}\t{}\t{}\t".format(cur, tag, info))

    def __str__(self):
        return 'ConsoleLogger'

class MemoryRedup:
    """
    内存判断重复
    """
    def __init__(self):
        self.pool = set()

    def loaded(self, url):
        if url in self.pool:
            return True
        return False

    def load(self, url):
        self.pool.add(url)

    def __str__(self):
        return 'MemoryRedup'


class RedisRedup:
    """
    redis判断重复
    """
    NAME = 'spider_urls'

    def __init__(self, host='127.0.0.1', port=6379, db=0, password=None):
        self.pool = redis.ConnectionPool(host=host, port=port, db=db, password=password)
        self.r = redis.Redis(connection_pool=self.pool)

    def loaded(self, url, name=NAME):
        if self.r.sismember(name, url):
            return True
        return False

    def load(self, url, name=NAME):
        self.r.sadd(RedisRedup.NAME, url)


class ConsoleDao:
    """
    结果输出到控制台
    """
    def save(self, matrix):
        for line in matrix:
            print(line)


class FileDao:
    """
    结果输出到文件
    """
    def __init__(self, path="data.txt"):
        self.path = path

    def save(self, matrix):
        with open(self.path, 'w', encoding='utf8') as f:
            for line in matrix:
                f.write('\t'.join(line))
                f.write(os.linesep)


class MySQLDao:
    """
    结果输出到MySQL
    """
    def __init__(self, host="localhost", user="root", password="admin", database="test", table="data", port=3306, charset='utf8mb4'):
        self.db = pymysql.connect(host=host, user=user, password=password, database=database, port=port, charset=charset)
        self.c = self.db.cursor()
        self.table_name = table

    def save(self, matrix):
        try:
            fields = ",".join(matrix[0])
            for index in range(1, len(matrix)):
                # 执行sql语句
                sql = u"""INSERT INTO %s(%s) VALUES (%s)"""%(self.table_name, fields,  ",".join(['"'+pymysql.escape_string(v)+'"' for v in matrix[index]]))
                self.c.execute(sql)
            # 提交到数据库执行
            self.db.commit()
        except Exception as e:
            # 如果发生错误则回滚
            self.db.rollback()
            raise e

    def __str__(self):
        return 'MySQLDao'

    def __del__(self):
        if self.c:
            self.c.close()
        if self.db:
            self.db.close()


class WordPressDao:
    """
    结果发布到wordpress
    """
    def __init__(self, host='localhost', user='root', password='admin'):
        self.host = host
        self.user = user
        self.password = password

    def save(self, matrix):
        for article in matrix[1:]:
            try:
                data = {
                    'title': article[0],
                    'content': article[1],
                    'status': "publish",
                    'comment_status': "open"
                }
                auth = str.encode('{}:{}'.format(self.user, self.password))
                headers = {'Authorization': 'Basic ' + str(base64.b64encode(auth), 'utf-8')}
                resp = requests.post('http://{}/index.php/wp-json/wp/v2/posts'.format(self.host), headers=headers, data=data)
                if resp.status_code>201:
                    raise Exception(resp)
            except Exception as e:
                raise e

    def __str__(self):
        return 'WordPressDao'

class Spider:
    def __init__(self, redup=MemoryRedup(), dao=ConsoleDao(), logger=ConsoleLogger()):
        """
        :param redup: 判断重复的类，必须生成对象，可选类有MemoryRedup、RedisRedup
        :param dao: 持久化的类，必须生成对象，可选类有ConsoleDao、FileDao、MySQLDao、WordPressDao
        :param logger: 日志类，必须生成对象，可选类有NoLogger、ConsoleLogger
        """
        self.pid = os.getpid()
        logger.log(tag='爬虫实例进程={}'.format(self.pid), info='redup={} dao={} logger={}'.format(redup, dao, logger))
        self.redup = redup
        self.dao = dao
        self.logger = logger

        self.alias = None
        self.start_site = None
        self.start_kwargs = {}
        self.next_kwargs = {}

        self.browser = asyncio.get_event_loop().run_until_complete(launch({'headless': True, 'args': ['--no-sandbox', '--disable-setuid-sandbox'], 'dumpio': True, 'slowMo': 1}))


    def list(self, alias, site, **kwargs):
        """
        列表网页
        :param alias: 网站名称，方便记忆
        :param site: 列表页地址
        :param kwargs: xpath表达式，必须使用url作为名称
        :return:
        """
        self.logger.log(alias, '网址 {}  list参数 {}'.format(site, kwargs))
        assert 'url' in kwargs.keys()
        self.alias = alias
        self.start_site = site
        self.start_kwargs = dict(kwargs)
        return self

    def article(self, **kwargs):
        """
        文章页面
        :param kwargs: xpath表达式
        :return:
        """
        self.logger.log(self.alias, 'article参数 {}'.format(kwargs))
        self.next_kwargs = dict(kwargs)
        return self

    async def __download_list(self, alias, site, kwargs):
        """
        下载列表
        :param alias:
        :param site:
        :param kwargs:
        :return:
        """
        self.logger.log(alias, '网址 {}  download_list参数 {}'.format(site, kwargs))
        start = time.time()
        try:
            assert isinstance(site, str)
            assert isinstance(kwargs, dict)
            page = await self.browser.newPage()
            try:
                await page.goto(site)
            except:
                self.logger.log(alias, '下载列表 {} 超时'.format(site), (time.time() - start))
            content = await  page.content()
            root = html.etree.HTML(content)
            for key, value in kwargs.items():
                result = root.xpath(value)
                kwargs[key] = result
            await page.close()
            self.logger.log(alias, '下载列表 {} 共计{}条'.format(site, len(list(kwargs.get(list(kwargs.keys())[0])))), (time.time() - start))
            return kwargs
        except:
            self.logger.log(alias, '下载列表{}报错  {}'.format(site, 'traceback.format_exc():\n%s' % traceback.format_exc()))
            return {}

    async def __download_article(self, alias, site):
        """
        下载文章
        :param alias:
        :param site:
        :return:
        """
        if self.redup.loaded(site):
            return {}
        kwargs = self.next_kwargs
        self.logger.log(alias, '网址 {}  download_article参数 {}'.format(site, kwargs))
        start = time.time()
        try:
            assert isinstance(site, str)
            assert isinstance(kwargs, dict)
            page = await self.browser.newPage()
            try:
                await page.goto(site, {'timeout': 10000})
            except:
                self.logger.log(alias, '下载文章 {} 超时'.format(site), (time.time() - start))
            content = await  page.content()
            root = html.etree.HTML(content)
            result = dict()
            for key, value in kwargs.items():
                result[key] = [''.join(root.xpath(value))]
            await page.close()
            self.logger.log(alias, '下载文章 {}'.format(site), (time.time() - start))
            return result
        except:
            self.logger.log(alias, '下载文章{}报错  {}'.format(site, 'traceback.format_exc():\n%s' % traceback.format_exc()))
            return {}

    def __save(self, url, values):
        assert values
        self.logger.log(self.alias, 'save参数 {}'.format(values))
        try:
            # 定义一个表
            matrix = []
            v_len = set()
            for key, values in values.items():
                v_len.add(len(values))

                m = list()
                m.append(key)
                for v in values:
                    m.append(v)
                matrix.append(m)
            if len(v_len) != 1:
                raise Exception('抓取字段的数量不一致')
            values = np.array(matrix).T
            self.dao.save(values)
            self.redup.load(url)
        except:
            self.logger.log(self.alias, '保存 {} 报错 {}'.format(url, '\r\ntraceback.format_exc():\n%s' % traceback.format_exc()))

    def run(self):
        """
        开始运行
        :return:
        """
        try:
            if self.start_kwargs:
                loop = asyncio.get_event_loop()
                values = asyncio.ensure_future(self.__download_list(self.alias, self.start_site, self.start_kwargs))
                loop.run_until_complete(values)
                values = values.result()

            if not self.next_kwargs:
                self.__save(self.start_site, values)

            if self.next_kwargs:
                for url in values.get('url'):
                    loop = asyncio.get_event_loop()
                    values = asyncio.ensure_future(self.__download_article(self.alias, url))
                    loop.run_until_complete(values)
                    values = values.result()
                    if values:
                        self.__save(url, values)
            self.__kill()
        except:
            self.logger.log(self.alias, '运行报错 {}'.format('traceback.format_exc():\n%s' % traceback.format_exc()))
            self.__kill()

    def __kill(self):
        try:
            # win平台
            if platform.system() == 'Windows':
                subprocess.Popen("taskkill /pid {} /f".format(self.pid), shell=True)
            # linux平台
            if platform.system() == 'Linux':
                os.kill(self.pid, signal.SIGKILL)
                self.browser.process.wait()
        except:
            self.logger.log(self.alias, '杀死进程报错 {}'.format('traceback.format_exc():\n%s' % traceback.format_exc()))


spider = Spider(dao=WordPressDao(host='hadoop100:86'))
spider.list('博客园精华', site="https://www.cnblogs.com/pick/", url="//a[@class='titlelnk']//@href").article(title="//a[@id='cb_post_title_url']//text()", content="//div[@id='cnblogs_post_body']//text()").run()