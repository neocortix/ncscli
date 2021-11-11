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
        scrapy.spiders.Rule(LinkExtractor(allow=(), deny=()), callback='parse_item', follow=True)
    ]

    def parse_item(self, response):
        title = response.css('title::text').get()
        yield { 'url': response.url, 'title': title }
