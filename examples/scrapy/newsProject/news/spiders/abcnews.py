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

def getUrlPathParts( url ):
    tup = urllib.parse.urlparse( url )
    return tup.path.split('/')

def abcnewsArticleFilter( url ):
    #url = row['url']
    forbidden = ['/news-missed-weekend/', '/major-headlines-start-week/'
                 '/start-here', '/business-highlights-']
    if anyFound( forbidden, url.lower() ):
        return False

    parts = getUrlPathParts( url )
    #print( 'parts:', parts )
    if len( parts ) <= 2:
        return False
    if parts[1] in [ '', 'abcnews', 'author', 'live', 'Site', 'topics', 'WNT' ]:
        return False
    return True


class AbcnewsSpider(CrawlSpider):
    name = 'abcnews'
    allowed_domains = ['abcnews.go.com']
    start_urls = [
        'https://abcnews.go.com',
        'https://abcnews.go.com/Business',
        'https://abcnews.go.com/Entertainment',
        'https://abcnews.go.com/Health',
        'https://abcnews.go.com/International',
        'https://abcnews.go.com/Lifestyle',
        'https://abcnews.go.com/Politics',
        'https://abcnews.go.com/Sports',
        'https://abcnews.go.com/Technology',
        'https://abcnews.go.com/US',
        ]

    rules = [
        scrapy.spiders.Rule(LinkExtractor(allow=(), deny=()), callback='parse_item')
    ]
    '''
    # original template-generated code
    rules = (
        Rule(LinkExtractor(allow=r'Items/'), callback='parse_item', follow=True),
    )
    '''
    outFilePath = name + '_out.csv'
    outFile = None

    def parse_item(self, response):
        self.logger.debug('parse_item url: %s', response.url)

        if abcnewsArticleFilter( response.url ):
            title = response.css('title::text').get()
            title = title.rsplit(' - ABC News', 1)[0]
            self.logger.debug('parse_item TITLE: %s', title )
            if self.outFile:
                print( response.url, enquote(title), file=self.outFile, sep=',' )
            yield { 'url': response.url, 'title': title }
