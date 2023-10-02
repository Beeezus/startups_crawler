# -*- coding: utf-8 -*-
import scrapy
from scrapy_core.items import DealsItem


class QlbDataSpider(scrapy.Spider):
    name = 'qlb_data'
    allowed_domains = ['www.quietlight.com']
    start_urls = ['https://quietlight.com/listings/']

    def __init__(self, date=None, run_count=0):
        self.run_count = run_count
        self.date = date
        super(QlbDataSpider, self).__init__()

    def parse(self, response):
        cards = self.extract_cards(response)
        for card in cards:
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
                site='qlb'
            )
            yield item

    def extract_cards(self, response):
        path = (
            '//div[@class="single-content under-loi all grid-item"] |'
            ' //div[@class="single-content public-listing all grid-item"]')
        return response.xpath(path)

    def _parse_title(self, card):
        x = './/h5/text()'
        return card.xpath(x).extract_first()

    def _parse_price(self, card):
        x = './/div[@class="price"]/strong/text()'
        return card.xpath(x).extract_first()

    def _parse_url(self, card):
        x = './/div/a/@href'
        return card.xpath(x).extract_first()

    def _parse_type(self, card):
        x = './/div[@class="recent_ecom"]/p/text()'
        return card.xpath(x).extract_first()

    def _parse_revenue(self, card):
        x = (
            './/div[@class="revenu_sec"]'
            '/p[contains(text(), "Revenue")]/text()'
        )
        return card.xpath(x).extract_first()

    def _parse_income(self, card):
        x = (
            './/div[@class="revenu_sec"]'
            '/p[contains(text(), "Income")]/text()'
        )
        return card.xpath(x).extract_first()

    def _parse_status(self, card):
        x = (
            './/div[contains(@class, "under_offer")]'
            '/a/span/text()'
        )
        return card.xpath(x).extract_first() or 'active'
