from spiderlib import *

spider = Spider("美国国防部", pipeline=FilePipeline("a.txt"))
spider.page(urls="https://www.defense.gov/Newsroom/", expresses={"title":"//a[@class='title']//text()", "link":"//a[@class='title']//@href"}, fields={"标题":"title", "链接":"link"})
spider.run()