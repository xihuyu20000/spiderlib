from spiderlib import *


def test01():
    spider = Spider("美国国防部")
    spider.list(urls="https://www.defense.gov/Newsroom/", expresses={"title":"//a[@class='title']//text()", "link":"//a[@class='title']//@href"}, fields={"标题":"title", "链接":"link"}, click_express=".more-row", click_times=0)
    spider.run()


def test02():
    spider = Spider("世界日报", downloader=RenderDownloader(), pipeline=FilePipeline("a.txt"))
    spider.list(urls="https://www.worldjournal.com/topic/%e5%8d%b3%e6%99%82now-2/?variant=zh-cn", expresses={"title":"//h2//a//text()", "link":"//h2//a//@href"}, next='link')
    spider.page(expresses={'title': '//h1//text()', 'content': "//div[@class='post-content']/p/text()"}, fields={"标题":"title", "链接":"content"})
    spider.run()


# test01()