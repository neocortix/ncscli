import urllib
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


class ExtracttitlesSpider(CrawlSpider):
    name = 'extractTitles'
    allowed_domains = ['neocortix.com']  # neocortix
    start_urls = ['https://loadtest-target.neocortix.com/']
    
    def __init__(self, *args, **kwargs):
        super(ExtracttitlesSpider, self).__init__(*args, **kwargs)
        startUrlOverridden = False
        startUrl = getattr( self, 'startUrl', None )
        if startUrl:
            parsed = urllib.parse.urlparse( startUrl )
            if parsed.scheme and parsed.netloc:
                self.start_urls = [startUrl]
                self.allowed_domains = [parsed.netloc]
                startUrlOverridden = True
            else:
                self.logger.warning( 'faulty startUrl given: %s', startUrl )
        if not startUrlOverridden:
            self.logger.info( 'using defualt start_urls (and domain): %s', self.start_urls )

    rules = [
        scrapy.spiders.Rule( LinkExtractor(allow=(), deny=()),  # 'raw.githubusercontent.com'
            callback='parse_item', follow=True
            )
    ]

    def parse_item(self, response):
        #self.logger.debug( 'response type: %s', type(response) )
        #self.logger.debug( 'response: %s', dir(response) )
        respheaders = response.headers  # .to_unicode_dict()
        #self.logger.debug( 'response.headers type: %s', type(respheaders) )
        #self.logger.debug( 'response.headers: %s', respheaders )
        contentType = respheaders.get( 'Content-Type' )
        #self.logger.debug( 'contentType: %s', contentType )
        isHtml = False
        #if contentType==b'text/html' or contentType=='text/html':
        if b'text/html' in contentType:  # or 'text/html' in contentType:
            #self.logger.debug( 'response is HTML')
            isHtml = True
        elif b'application/xml' in contentType:
            isXml = True
            if 'feedformat=atom' in response.url or 'feed=atom' in response.url:
                self.logger.info( 'ATOM feed: %s', response.url )
            else:
                self.logger.info( 'XML response: %s', response )
        else:
            if b'text/plain' not in contentType:
                self.logger.info( 'response is NOT HTML; content-type: %s, response type: %s',
                    contentType, type(response)
                    )
                self.logger.info( 'offending response: %s', response )
        if isHtml:
            title = response.css('title::text').get()
            yield { 'url': response.url, 'title': title }
        else:
            return []
