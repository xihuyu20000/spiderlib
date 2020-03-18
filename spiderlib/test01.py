import sys

from spiderlib import *

# spide = Spider("美国国防部")
# spide.page(urls="https://www.cnblogs.com", expresses={"link":"//a[@class='titlelnk']//@href", "title":"//a[@class='titlelnk']//text()"},
#            fields={"地址":"link", "标题":"title"}, is_list=True)
# spide.run()



def test_FilePipeline1():
    """
    结果写入到一个文件a.txt中
    :return:
    """
    spider = Spider('博客园精华', pipeline=FilePipeline('../a1.txt', sep="\t\t"))
    print(spider)
    spider.page(urls="https://www.cnblogs.com/pick/", expresses={"link":"//a[@class='titlelnk']//@href"}, next='link', fields={"网址":"link"}, is_list=True)
    spider.page(expresses={"title":"//a[@id='cb_post_title_url']//text()", "content":"//div[@id='cnblogs_post_body']//text()"}, fields={"标题":'title', "正文":"content"}, is_list=False)
    spider.run()


def test_FilePipeline2():
    """
    结果写入到两个文件a1.txt和a2.txt中
    todo 输出pid，只有最后一个有，很奇怪
    :return:
    """
    spider = Spider('博客园精华', downloader=RenderDownloader(), pipeline=FilePipeline('../a21.txt'))
    spider.page(urls="https://www.cnblogs.com/pick/", expresses={"link":"//a[@class='titlelnk']//@href"}, next='link', fields={"网址":"link"}, is_list=True)
    spider.page(expresses={"title":"//a[@id='cb_post_title_url']//text()", "content":"//div[@id='cnblogs_post_body']//text()"}, fields_tag='../a22.txt', fields={"标题":'title', "正文":"content", "上级索引":"pid"}, is_list=False)
    spider.run()


def test_FilePipeline3():
    """
    测试常量表达式，结果写入到一个文件a3.txt
    :return:
    """
    spider = Spider('博客园精华', pipeline=FilePipeline('../a3.txt', sep="\t\t"))
    print(spider)
    spider.page(urls="https://www.cnblogs.com/pick/", expresses={"link":"//a[@class='titlelnk']//@href", "时间戳":5656567567567}, fields={"网址":"link", "时间戳":"时间戳"}, is_list=True)
    spider.run()


def test_FilePipeline4():
    """
    测试常量保存值，结果写入到一个文件a4.txt
    :return:
    """
    spider = Spider('博客园精华', pipeline=FilePipeline('../a4.txt', sep="\t\t"))
    cur = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    spider.page(urls="https://www.cnblogs.com/pick/", expresses={"link":"//a[@class='titlelnk']//@href"}, fields={"网址":"link", "时间戳":cur}, is_list=True)
    spider.run()

def test_MySQLPipeline1():
    """
    结果写入到
    :return:
    """
    spider = Spider('博客园精华', pipeline=MySQLPipeline())
    spider.page(urls="https://www.cnblogs.com/pick/", expresses={"link":"//a[@class='titlelnk']//@href"}, next='link', is_list=True)
    spider.page(expresses={"title":"//a[@id='cb_post_title_url']//text()", "content":"//div[@id='cnblogs_post_body']//text()"}, fields={"title":'title', "text":"content", "url":"pid"}, is_list=False)
    spider.run()


def test_WordPressPipeline1():
    """
    结果写入到
    :return:
    """
    spider = Spider('博客园精华', pipeline=WordPressPipeline(host='192.168.1.88:84'))
    spider.page(urls="https://www.cnblogs.com/pick/", expresses={"link":"//a[@class='titlelnk']//@href"}, next='link', is_list=True)
    spider.page(expresses={"title":"//a[@id='cb_post_title_url']//text()", "content":"//div[@id='cnblogs_post_body']//text()"}, fields={"title":'title', "content":"content"}, is_list=False)
    spider.run()


def test_aaa():
    spide = Spider("美国国防部", pipeline=FilePipeline("../data.txt"))
    spide.page(urls="https://www.cnblogs.com",
               expresses={"link": "//a[@class='titlelnk']//@href", "title": "//a[@class='titlelnk']//text()"},
               fields={"地址": "link", "标题": "title"}, is_list=True)
    spide.run()

if __name__ == '__main__':
    # test_FilePipeline1()
    test_FilePipeline2()
    # test_FilePipeline3()
    # test_FilePipeline4()
    # test_MySQLPipeline1()
    # test_WordPressPipeline1()
    # test_aaa()