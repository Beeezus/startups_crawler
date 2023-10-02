# -*- coding: utf-8 -*-
import scrapy
from scrapy_core.items import DealsItem


class AbbDataSpider(scrapy.Spider):
    name = 'abb_data'
    allowed_domains = ['http://www.appbusinessbrokers.com']
    start_urls = ['http://www.appbusinessbrokers.com/buy-business/']

    def __init__(self, date, run_count=0):
        self.run_count = run_count
        self.date = date
        super(AbbDataSpider, self).__init__()

    def parse(self, response):
        cards = self.extract_card(response)
        for card in cards or []:
            item = DealsItem(
                title=self._parse_title(card),
                url=self._parse_url(card),
                price=self._parse_price(card),
                type=self._parse_type(card),
                revenue=self._parse_revenue(card),
                income=self._parse_income(card),
                status=self._parse_status(card),
                run=self.run_count,
                date=self.date,
                site='abb'
            )
            yield item

    def extract_card(self, response):
        path = (
            '//div[@class="properties-items isotope"]'
            '/div[@class="items-list"]/div'
        )
        return response.xpath(path)

    def _parse_title(self, card):
        x = './/h3[@class="property-box-title"]/a/text()'
        return card.xpath(x).extract_first()

    def _parse_price(self, card):
        x = self._price_path('hf-property-bedroom')
        return card.xpath(x).extract_first()

    def _parse_url(self, card):
        x = './/h3[@class="property-box-title"]/a/@href'
        return card.xpath(x).extract_first()

    def _parse_type(self, card):
        x = (
            './/div[@class="property-box-price"]'
            '/div/div/div[contains(@class, "field-value")]/text()'
        )
        return card.xpath(x).extract_first()

    def _parse_revenue(self, card):
        x = self._price_path('hf-property-bathroom')
        return card.xpath(x).extract_first()

    def _parse_income(self, card):
        x = self._price_path('hf-property-garages')
        return card.xpath(x).extract_first()

    def _parse_status(self, card):
        return 'active'

    def _price_path(self, path):
        return(
            f'.//div[contains(@class, "{path}")]'
            '/div/div[contains(@class, "field-value")]/text()'
        )
