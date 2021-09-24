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

def articleFilter( url ):
    #logger.info( '' )
    path = getUrlPath( url )
    bad = ['-deals-', '-deals/', '/onpolitics/', '-news-you-missed-', 
        '-more-weekend-news/', '/reviewedcom/'
        ]
    if anyFound( bad, path.lower() ):
        return False
    if path.startswith( '/story/' ):
        return True
    return False

class UsatodaySpider(CrawlSpider):
    name = 'usatoday'
    allowed_domains = ['usatoday.com']
    start_urls = [
        'https://www.usatoday.com/',
        'https://www.usatoday.com/life/',
        'https://www.usatoday.com/money/',
        'https://www.usatoday.com/news/',
        'https://www.usatoday.com/sports/',
        'https://www.usatoday.com/tech/',
        'https://www.usatoday.com/travel/',
        'https://www.usatoday.com/washington/',
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
            self.logger.debug('parse_item TITLE: %s', title )
            if self.outFile:
                print( response.url, enquote(title), file=self.outFile, sep=',' )
            yield { 'url': response.url, 'title': title }
