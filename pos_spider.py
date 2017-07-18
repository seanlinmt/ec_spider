# -*- coding: utf-8 -*-
import scrapy
import logging
from scrapy.contrib.loader import ItemLoader
from scrapy.contrib.loader.processor import MapCompose, Join
from scrapy.exceptions import CloseSpider
import settings
from items import LocationItem, LocationItem_Fields
from models import DBSession, LocationPostcode

min_postcode = 1000
max_postcode = 98859
max_results = 9999
max_retries = 10
known_postcodes = [97014, 97015, 97100, 97200, 97300, 98000, 98007, 98008, 98009, 98050, 98057, 98058, 98059, 98100,
                   98107, 98109, 98150, 98157, 98159, 98200, 98300, 98700, 98707, 98708, 98709, 98750, 98800, 98850,
                   98857, 98859]


def get_postcode():
    foundCode = False
    session = DBSession()
    try:
        for code in known_postcodes:
            existing = session.query(LocationPostcode.postcode).filter(
                LocationPostcode.postcode == "{:05d}".format(code)).first()
            if not existing:
                foundCode = code
                break
    finally:
        session.close()

    return foundCode


class POS_spider(scrapy.Spider):
    name = settings.POS_BOT_NAME
    allowed_domains = ["pos.com.my"]
    start_urls = ("http://www.pos.com.my/pos/appl/location-finder.asp",)
    results_xpath = '//table[@class="tbl01"]/tr/td/table/tr'
    doesnot_exist_xpath = '//table[@class="tbl01"]/tr/td/table/tr[2]/td/center/text()'
    singlepage_result_url = "http://www.pos.com.my/pos/appl/viewlocation.asp?Page_=1&from_page=same&list_first=1&list_last={}&total_page=1&SelectDay=".format(
        10000)

    def __init__(self, **kwargs):
        self.retries = 0
        self.postcode = None

        super(POS_spider, self).__init__(**kwargs)

    def parse(self, response):
        self.postcode = get_postcode()
        logging.info("{} FOUND. WORKING ON THIS NOW.".format(self.postcode))
        if self.postcode >= max_postcode:
            raise CloseSpider("max postcode reached")

        if self.retries > max_retries:
            raise CloseSpider("max retries: {}".format(self.retries))

        return scrapy.FormRequest.from_response(
            response,
            formdata={'postcode': "{:05d}".format(self.postcode),
                      'x': '81',
                      'y': '2'},
            callback=self.after_session_init
        )

    def restart(self):
        return scrapy.Request(url=self.start_urls[0])

    def after_session_init(self, response):
        # check if an error message was returned
        error_result = response.selector.xpath(self.doesnot_exist_xpath).extract()

        if len(error_result) == 2:
            for entry in error_result:
                if entry.find("exist") != -1:
                    return self.restart()

        # send GET with all results in one page
        return scrapy.Request(self.singlepage_result_url, callback=self.parse_page)

    def parse_page(self, response):
        result_list = response.selector.xpath(self.results_xpath)[1:]
        if len(result_list) >= max_results:
            raise CloseSpider("{} returns too many results".format(self.postcode))

        for result in result_list:
            loader = ItemLoader(LocationItem(), selector=result)
            loader.default_input_processor = MapCompose(unicode.strip)
            loader.default_output_processor = Join()

            for field, xpath in LocationItem_Fields.iteritems():
                loader.add_xpath(field, xpath)

            yield loader.load_item()

        self.restart()
