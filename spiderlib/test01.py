from spiderlib import *


def test_FilePipeline1():
    """
    结果写入到一个文件a.txt中
    :return:
    """
    os.remove("../a1.txt")
    spider = Spider('博客园精华', pipeline=FilePipeline('../a1.txt'))
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
    os.remove("../a21.txt")
    os.remove("../a22.txt")
    spider = Spider('博客园精华', pipeline=FilePipeline('../a21.txt'))
    spider.page(urls="https://www.cnblogs.com/pick/", expresses={"link":"//a[@class='titlelnk']//@href"}, next='link', fields={"网址":"link"}, is_list=True)
    spider.page(expresses={"title":"//a[@id='cb_post_title_url']//text()", "content":"//div[@id='cnblogs_post_body']//text()"}, fields_tag='../a22.txt', fields={"标题":'title', "正文":"content", "上级索引":"pid"}, is_list=False)
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


if __name__ == '__main__':
    # test_FilePipeline2()
    # test_MySQLPipeline1()

    test_WordPressPipeline1()