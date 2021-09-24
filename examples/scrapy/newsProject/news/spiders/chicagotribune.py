import urllib

import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

def anyFound( a, b ):
    ''' return true iff any items from iterable a is found in iterable b '''
    for x in a:
        if x in b:
            return True
    return False

def enquote( txt ):
    return '"' +txt+ '"'

def getUrlPath( url ):
    tup = urllib.parse.urlparse( url )
    return tup.path

def getUrlPathParts( url ):
    tup = urllib.parse.urlparse( url )
    return tup.path.split('/')

def chicagotribuneArticleFilter( url ):
    path = getUrlPath( url )
    if ('/news/nationworld/' in path) or ('/nation-world/' in path):
        return True
    return False


class ChicagotribuneSpider(CrawlSpider):
    name = 'chicagotribune'
    allowed_domains = ['chicagotribune.com']
    start_urls = ['https://www.chicagotribune.com/nation-world/']

    rules = [
        scrapy.spiders.Rule(LinkExtractor(allow=(), deny=()), callback='parse_item')
    ]
    outFilePath = name + '_out.csv'
    outFile = None

    def parse_item(self, response):
        self.logger.debug('parse_item url: %s', response.url)
        if chicagotribuneArticleFilter( response.url ):
            title = response.css('title::text').get()
            title = title.rsplit(' - Chicago Tribune', 1)[0]
            self.logger.debug('parse_item TITLE: %s', title )
            if self.outFile:
                print( response.url, enquote(title), file=self.outFile, sep=',' )
            yield { 'url': response.url, 'title': title }
