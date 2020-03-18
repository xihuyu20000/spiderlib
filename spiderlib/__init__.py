from typing import Union, Optional

name = 'spiderlib'
author = '吴超  QQ 377486624'

import re
import subprocess
import time
import traceback
import os
import platform
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


class ConsoleLogger(NoLogger):
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


class MemoryRedup():
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
        print("新增去重 {}".format(url))
        self.pool.add(url)

    def __str__(self):
        return 'MemoryRedup'


class RedisRedup(MemoryRedup):
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

    def loaded(self, url, name=NAME):
        if self.r.sismember(name, url):
            return True
        return False

    def load(self, url, name=NAME):
        print("新增去重 {}".format(url))
        self.r.sadd(RedisRedup.NAME, url)

    def __str__(self):
        return 'RedisRedup'


class ConsolePipeline:
    """
    结果输出到控制台
    """
    def save(self, values, tag):
        print("=======================================输出结果到控制台=======================================")
        for line in values:
            print(line)

    def __str__(self):
        return 'ConsolePipeline'


class FilePipeline(ConsolePipeline):
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

class MySQLPipeline(ConsolePipeline):
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

    def save(self, values, table_name: str = ''):
        """
        保存
        :param values:
        :return: 有错抛异常
        """
        sql = ''
        t_name = table_name if table_name else  self.table_name
        try:
            fields = ",".join(values[0])
            for index in range(1, len(values)):
                # 执行sql语句
                sql = u"""INSERT INTO %s(%s) VALUES (%s)"""%(t_name, fields,  ",".join(['"'+pymysql.escape_string(v)+'"' for v in values[index]]))
                self.c.execute(sql)
            # 提交到数据库执行
            self.db.commit()
        except Exception as e:
            print(sql)
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


class WordPressPipeline(ConsolePipeline):
    """
    结果发布到WordPress。输入的字段必须是title和content
    """
    def __init__(self, host:str='localhost', user:str='root', password:str='admin'):
        self.host = host
        self.user = user
        self.password = password

    def save(self, values, tag: str = '') -> None:
        """
        保存
        :param values:
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
                resp = requests.post('http://{}/index.php/wp-json/wp/v2/posts'.format(self.host), headers=headers, data=data, timeout=5)
                if resp.status_code>201:
                    raise Exception(resp)
            except Exception as e:
                raise e

    def __str__(self):
        return 'WordPressPipeline'


class MemoryScheduler:
    """
    内存调度器
    """
    def __init__(self, maxsize=0):
        self.q = list()

    def put(self, ele)->bool:
        """
        插入到尾部
        :param ele:
        :return:
        """
        self.q.append(ele)
        return True

    def head(self):
        """
        只读取，不删除
        :return:
        """
        return self.q[0] if len(self.q) else None

    def remove_head(self):
        """
        删除头部元素，并返回
        :return:
        """
        ele = self.q.pop(0)
        print("调度器  剩余容量{}  删除元素{}".format(self.len(), ele))
        return ele

    def len(self)->int:
        return len(self.q)

    def __str__(self):
        return 'MemoryScheduler'


class Template:
    def __init__(self, urls, expresses, next, fields_tag, fields, is_list, hooker):
        """
        :param urls:
        :param expresses:
        :param next:
        :param fields_tag: 表名，表示每个模板可以保存到不同的表中
        :param fields:
        :param is_list: True表示列表页，False表示实体页
        :param hooker:
        """
        assert urls
        assert isinstance(urls, list)
        assert expresses
        self.urls = urls
        self.expresses = expresses
        self.next = next
        self.fields_tag = fields_tag
        self.fields = fields
        self.is_list = is_list
        self.hooker = hooker
        self.child:Template = None  #下一级的模板

    def __str__(self):
        return '({}Template:  urls={}  expresses={}  next={}  fields_tag={}  fields={} is_list={} hooker={} child={})'.format('列表' if self.is_list else '实体', self.urls, self.expresses, self.next, self.fields_tag, self.fields, self.is_list, self.hooker, self.child)
    __repr__ = __str__


class Page:
    def __init__(self, parent: Optional[str], url: str, template: Template):
        """
        初始化方法，很重要。parent是上级页面的url
        :param parent: 上级页面的url
        :param url:  本页面的url
        :param template: 本页面对应的模板
        """
        self.parent = parent
        self.url = url
        self.template = template
        self.values: dict = {}    #抓取页面后的值，保存到这里

    def __str__(self):
        return '(Page: parent={}  url={}  values={}  template={})'.format(self.parent, self.url, self.values, self.template)
    __repr__ = __str__


class Downloader:
    """
    下载器
    """

    async def download(self, spider, page: Page) -> bool:
        return True

    def __str__(self):
        return 'Downloader'


class SimpleDownloader(Downloader):
    """
    使用SimpleDownloader渲染后下载页面
    """

    async def download(self, spider, page: Page, method:str = 'get', data=None, json=None, params=None, **kwargs) -> bool:
        """
        使用requests模块下载
        :param spider:
        :param page:
        :param method: 只能是get或者post
        :param data: 参考requests模块的post方法参数
        :param json:参考requests模块的post方法参数
        :param params:参考requests模块的get和post方法参数
        :param kwargs:参考requests模块的get和post方法参数
        :return:
        """
        try:
            start = time.time()

            response = ''
            try:
                if method == 'get':
                    response = requests.get(page.url, params=params, kwargs=kwargs, timeout=5)
                if method == 'post':
                    response = requests.post(page.url, data=data, json=json, kwargs=kwargs, timeout=5)
            except:
                spider.logger.log(spider.alias, '下载列表 {} 超时'.format(page.url), (time.time() - start))
            content = response.content if response else ''
            root_element = html.etree.HTML(content)
            for key, value in page.template.expresses.items():
                value = str(value)
                if value.startswith("/"):
                    content = root_element.xpath(value) if root_element else ''
                    if not page.template.is_list:
                        content = ["".join(content)]
                    page.values[key] = content
            #抓取的记录条数
            rows = len(list(page.values.get(list(page.values.keys())[0])))
            #表达式，不是/开头；那就是常量
            for key, value in page.template.expresses.items():
                value = str(value)
                if not value.startswith("/"):
                    page.values[key] = [str(value) for i in range(rows)]

            page.template.hooker.after_download(page)
            spider.logger.log(spider.alias, '下载列表 {} 共计{}条 '.format(page.url, rows),(time.time() - start))
        except:
            spider.logger.log(spider.alias,
                            '下载列表{}报错  {}'.format(page.url, 'traceback.format_exc():\n%s' % traceback.format_exc()))
        return True

    def __str__(self):
        return 'SimpleDownloader'


class RenderDownloader(Downloader):
    """
    使用PyppeteerDownloader渲染后下载页面
    """

    async def download(self, spider, page: Page) -> bool:
        try:
            start = time.time()

            browser_page = await spider.browser.newPage()
            try:
                await browser_page.goto(page.url, options={'timeout':10000})
            except:
                spider.logger.log(spider.alias, '下载列表 {} 超时'.format(page.url), (time.time() - start))
            content = await  browser_page.content()
            root_element = html.etree.HTML(content)
            for key, value in page.template.expresses.items():
                value = str(value)
                if value.startswith("/"):
                    content = root_element.xpath(value)
                    if not page.template.is_list:
                        content = ["".join([item.strip() for item in content])]
                    page.values[key] = content
            #抓取的记录条数
            rows = len(list(page.values.get(list(page.values.keys())[0])))
            #表达式，不是/开头；那就是常量
            for key, value in page.template.expresses.items():
                value = str(value)
                if not value.startswith("/"):
                    page.values[key] = [str(value) for i in range(rows)]
            await browser_page.close()

            page.template.hooker.after_download(page)
            spider.logger.log(spider.alias, '下载列表 {} 共计{}条 '.format(page.url, rows),(time.time() - start))
        except:
            spider.logger.log(spider.alias,
                            '下载列表{}报错  {}'.format(page.url, 'traceback.format_exc():\n%s' % traceback.format_exc()))
        return True

    def __str__(self):
        return 'PyppeteerDownloader'


class Hooker:
    """
    专门用于hook的类
    """
    def before_download(self, page: Page)->None:
        """
        下载页面之前
        :param page:
        :return:
        """
        pass

    def after_download(self, page: Page)->None:
        """
        下载页面之后
        :param page:
        :return:
        """
        pass

    def before_save(self, page: Page)->None:
        """
        保存数据之前
        :param page:
        :return:
        """
        pass


class Spider:
    url_regex = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    def __init__(self, alias: str, downloader: Downloader = SimpleDownloader(), redup: MemoryRedup = MemoryRedup(), scheduler: MemoryScheduler = MemoryScheduler(), pipeline: ConsolePipeline = ConsolePipeline(), logger: ConsoleLogger = ConsoleLogger()):
        """
        实例化爬虫类，各个参数很重要，需要认真填写
        :param alias: 网站名称，方便记忆
        :param pattern: 运行模式，可选值有1、2。值1表示使用简洁模式，值2表示使用渲染模式
        :param redup: 判断重复的类，必须创建对象，可选类有MemoryRedup、RedisRedup
        :param scheduler: 调度器类，必须创建对象，可选类有MemoryScheduler
        :param pipeline: 持久化的类，必须创建对象，可选类有ConsoleDao、FilePipeline、MySQLPipeline、WordPressPipeline
        :param logger: 日志类，必须创建对象，可选类有NoLogger、ConsoleLogger
        """
        self.pid = os.getpid()
        logger.log(tag='爬虫实例 进程={}'.format(self.pid), info='downloader={} redup={} scheduler={} pipeline={} logger={}'.format(downloader, redup, scheduler, pipeline, logger))
        self.alias = alias
        self.downloader = downloader
        self.redup = redup
        self.scheduler = scheduler
        self.pipeline = pipeline
        self.logger = logger
        self.browser = asyncio.get_event_loop().run_until_complete(launch({'headless': True, 'args': ['--no-sandbox', '--disable-setuid-sandbox'], 'dumpio': True, 'slowMo': 1}))
        self.template = None    #保存本页面对应的模板
        self.save_count = 0 #保存成功的数量

    def page(self, urls:Union[list, str] = '', is_list: bool = False, expresses: dict = {}, fields_tag: str = '', fields: dict = {}, next: str = '', hooker: Hooker = Hooker()):
        """
        抓取信息配置
        :param urls: 被抓取的url列表，可以是list，也可以是str
        :param is_list: bool，是否是文章或者新闻。如果是列表，写True；如果是具体的数据，写False
        :param expresses: dict，抓取的字段和xpath。如果表达式，不是/开头；那就是常量
        :param fields_tag: str，表名或者文件名。不同内容对应到不同的输出
        :param fields: dict，保存时指定的名称，抓取的字段名与保存的字段名之间的映射关系。
        如果不填写，那么就不会保存数据。
        抓取的时候，有个字段是pid，表示前一页面的url。
        也可以设置常量字段，比如时间戳之类的
        :param next: str，传递给下一级抓取时，指定的字段名，这个字段名一定出现在expresses的key中。如果url需要补全，在这里可以实现
        :param hooker: Hooker 用于hook
        :return: self
        """
        self.logger.log(self.alias, 'page(...)参数 urls={} expresses={} fields_tag={} fields={} next={} is_list={} hooker={}'.format(urls, expresses, fields_tag, fields, next, is_list, hooker))
        urls = [urls] if isinstance(urls, str) else urls
        t = Template(urls, expresses, next, fields_tag, fields, is_list, hooker)
        if self.template:
            self.template.child = t
        else:
            self.template = t
        return self

    async def __download(self, page: Page)->bool:
        """
        下载页面
        :param page:
        :return: 重复url，返回False；否则，返回True
        """
        self.logger.log(self.alias, '__download(...)参数 {}'.format(page))
        page.template.hooker.before_download(page)
        url = page.url
        if self.redup.loaded(url):
            return False
        assert re.match(Spider.url_regex, url)
        return await self.downloader.download(self, page)

    def __pre_save(self, page: Page)->None:
        self.logger.log(self.alias, '__pre_save(...)参数 {}'.format(page))

        #使用fields内容
        items = {}
        if page.template.fields:
            for k,v in page.template.fields.items():
                # k是保存用的名字，v是抓取时用的名字
                if v in page.values.keys(): #pid不是抓取字段，这里必须判断
                    items[k] = page.values[v]
        else:
            items = page.values

        # 定义一个二维数组
        matrix = []
        v_len = set()
        for key, values in items.items():
            v_len.add(len(values))
            m = []
            m.append(key)
            for value in values:
                m.append(value)
            matrix.append(m)
        if page.template.is_list and len(v_len) != 1:
            raise Exception(self.alias +' 抓取字段的数量不一致 ' + str(page.values))

        # 后面会根据key取值，所以Page对象增加一个新的属性
        page.matrix = np.array(matrix).T.tolist()

        # 判断是否需要pid字段
        key_pid = None
        for k,v in page.template.fields.items():
            if "pid" == v:
                key_pid = k
                break
        if key_pid:
            page.matrix[0].append(key_pid)
            for vlist in page.matrix[1:]:
                vlist.append(page.parent)

        # 判断常量字段，指的是在expresses的key中没有出现过
        expresses_keys = page.template.expresses.keys()
        for k,v in page.template.fields.items():
            #不在，就是常量
            if not str(v) in expresses_keys:
                page.matrix[0].append(k)
                for vlist in page.matrix[1:]:
                    vlist.append(str(v))

        #钩子函数
        page.template.hooker.before_save(page)

    def __save(self, page: Page)->bool:
        """
        保存
        :param page:
        :return:
        """
        self.logger.log(self.alias, '__save(...)参数 {}'.format(page))
        try:
            self.pipeline.save(page.matrix, page.template.fields_tag)
            self.save_count = self.save_count+1
            return True
        except:
            self.logger.log(self.alias, '保存报错 {}'.format('traceback.format_exc():\n%s' % traceback.format_exc()))
            return False

    def __after_save(self, page: Page, flag:bool)->None:
        """
        保存后，修改调度器信息
        :param node:
        :return:
        """
        self.logger.log(self.alias, '__after_save(...)参数 page={} flag={}'.format(page, flag))
        # 1、成功操作后，加入到去重队列
        if flag:
            self.redup.load(page.url)
        # 2、取出url
        if page.template.next and page.template.child:
            next_urls = page.values[page.template.next]
            for next_url in next_urls:
                # 3、添加新的到队列
                self.scheduler.put(Page(parent=page.url, url=next_url, template=page.template.child))
        # 4、删除头元素
        self.scheduler.remove_head()
        # 5、日志
        self.logger.log(self.alias, '成功保存数据{}条 '.format(self.save_count))

    def run(self):
        """
        开始运行
        :return:
        """
        self.logger.log(self.alias, 'run(...)开始运行')
        # 生成种子
        for url in self.template.urls:
            self.scheduler.put(Page(parent=None, url=url, template=self.template))

        event_loop = asyncio.get_event_loop()
        try:
            while self.scheduler.len():
                page = self.scheduler.head()
                f = asyncio.ensure_future(self.__download(page))
                event_loop.run_until_complete(f)
                normal_flag = True
                if f.result() and page.template.fields:
                    self.__pre_save(page)
                    normal_flag = self.__save(page)
                self.__after_save(page, normal_flag)
            self.logger.log(self.alias, '运行结束')
        except:
            self.logger.log(self.alias, '运行报错 {}'.format('traceback.format_exc():\n%s' % traceback.format_exc()))
        finally:
            self.logger.log(self.alias, '清理环境')
            self.__kill()

    def __kill(self):
        """
        清理环境
        :return:
        """
        self.logger.log(self.alias, '关闭浏览器')
        try:
            # win平台
            if platform.system() == 'Windows':
                subprocess.Popen("taskkill /pid {} /f".format(self.pid), shell=True)
            # linux平台
            if platform.system() == 'Linux':
                os.system("ps -ef |grep chrome |awk '{print $2}'|xargs kill -9")
        except:
            pass
