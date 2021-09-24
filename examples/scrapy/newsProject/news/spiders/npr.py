import urllib

import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

def enquote( txt ):
    return '"' +txt+ '"'

def getUrlPath( url ):
    tup = urllib.parse.urlparse( url )
    return tup.path

def articleFilter( url ):
    path = getUrlPath( url )
    if path.startswith( '/2021/' ) or path.startswith( '/2022/' ):
        return True
    return False

class NprSpider(CrawlSpider):
    name = 'npr'
    allowed_domains = ['npr.org']
    start_urls = [
        'https://www.npr.org/',
        'https://www.npr.org/sections/news/',
        'https://www.npr.org/sections/arts/',
        'https://www.npr.org/music/',
        'https://www.npr.org/sections/national/',
        'https://www.npr.org/sections/world/',
        'https://www.npr.org/sections/politics/',
        'https://www.npr.org/sections/business/',
        'https://www.npr.org/sections/technology/',
        'https://www.npr.org/sections/science/',
        'https://www.npr.org/sections/health/',
        ]

    rules = [
        scrapy.spiders.Rule(LinkExtractor(allow=(), deny=()), callback='parse_item')
    ]
    outFilePath = name + '_out.csv'
    outFile = None

    def parse_item(self, response):
        self.logger.debug('parse_item url: %s', response.url)
        if articleFilter( response.url ):
            title = response.css('title::text').get()
            title = title.rsplit(' : NPR', 1)[0]
            title = title.strip()
            self.logger.debug('parse_item TITLE: %s', title )
            if self.outFile:
                print( response.url, enquote(title), file=self.outFile, sep=',' )
            yield { 'url': response.url, 'title': title }
