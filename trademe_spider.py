import scrapy
import re

class TrademeSpider(scrapy.Spider):
    name = "trademe"
    allowed_domains = ["www.trademe.co.nz"]
    start_urls = ["https://www.trademe.co.nz/a/property/residential/sale/auckland/search"]
    page_count = 1
    max_page = 456

    def parse(self, response):
        for href in response.css(r".o-card a[href*='property/residential']::attr(href)").extract():
            url = response.urljoin(href)
            print(url)
            req = scrapy.Request(url, callback=self.parse_listing)
            yield req
        
        # follow next page links
        if self.page_count <= self.max_page:
            self.page_count += 1
            next_page_url = self.start_urls[0] + '?page=' + str(self.page_count)
            request = scrapy.Request(next_page_url)
            yield request

    def parse_listing(self, response):
        for sel in response.css('html').extract():
            data = {}

            # listing id
            match = re.search('listing/(\d.+)\?', response.url)
            data['id'] = match.group(1)

            data['address'] = response.css("h1::text").extract()

            property_attribs = response.css(".tm-property-listing-attribute-tag__tag--content::text").extract()
            try:
                data['bedrooms'] = property_attribs[0].strip()
            except:
                data['bedrooms'] = ''
            try:
                data['baths'] = property_attribs[1].strip()
            except:
                data['baths'] = ''
            try:
                data['land'] = property_attribs[2].strip()
            except:
                data['land'] = ''

            data['type'] = response.css(".o-table td::text").extract_first().strip()

            #get gps coordinates
            try:
                match = re.search("lat=(-?\d+\.\d+)&long=(-?\d+\.\d+)", response.css(".tm-property-insights-banner__link::attr(href)").extract()[0])
                data['lat'] = match.group(1) 
                data['long'] = match.group(2)
            except:
                data['lat'] = ''
                data['long'] = ''

            data['price'] = response.css(".tm-property-listing-body__price ::text").extract()
        yield data