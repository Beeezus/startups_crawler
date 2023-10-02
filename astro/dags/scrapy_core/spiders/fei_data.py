# -*- coding: utf-8 -*-
import scrapy
from scrapy_core.items import DealsItem
from fuzzywuzzy import process
import re
import pendulum


class FeiItem(DealsItem):
    listing_date = scrapy.Field()
    internal_id = scrapy.Field()

class FeiDataSpider(scrapy.Spider):
    name = 'fei_data'
    allowed_domains = ['feinternational.com']
    start_urls = ['https://feinternational.com/buy-a-website/']

    choices = [
        'saas', 'subscription', 'display advertising',
        'AdSense', 'lead gen', 'affiliate', 'blog',
        'forum', 'review', 'Ecommerce', 'E-commerce'
    ]

    def __init__(self, date=None, run_count=0):
        self.run_count = run_count
        self.date = date
        super(FeiDataSpider, self).__init__()

    def parse(self, response):
        cards = self.extract_card(response)
        for card in cards or []:
            item = FeiItem(
                title=self._parse_title(card),
                url=self._parse_listing_url(card),
                price=self._parse_price(card),
                type=self._parse_type(card),
                revenue=self._parse_revenue(card),
                income=self._parse_income(card),
                status=self._parse_status(card),
                id=self._parse_id(card),
                run=self.run_count,
                date=self.date,
                listing_date=self._listing_date(card),
                internal_id=self._parse_internal_id(card),
                site='fei'
            )
            yield item

    def _listing_date(self, card):
        x = './/article/@data-date'
        listing_date = card.xpath(x).extract_first()
        dt = pendulum.parse(listing_date)
        return dt.to_date_string() if dt else None

    def _parse_id(self, card):
        internal_id = self._parse_internal_id(card)
        return f'fei-{internal_id}'

    def _parse_internal_id(self, card):
        url = self._parse_listing_url(card)
        pattern = r'.*/([0-9]+)-[^/]*$'
        found = re.search(pattern, url)
        if found:
            return found.group(1)

    def extract_card(self, response):
        path = '//div[@class="tab"]/div[@class="listing"]'
        return response.xpath(path)

    def _parse_title(self, card):
        x = './/h2[@class="listing-title"]/a/text()'
        return card.xpath(x).extract_first()

    def _parse_price(self, card):
        x = '//dd[contains(@class, "asking-price")]/text()'
        return card.xpath(x).extract_first()

    def _parse_listing_url(self, card):
        x = './/h2[@class="listing-title"]/a/@href'
        return card.xpath(x).extract_first()

    def _parse_type(self, card):
        title = self._parse_title(card)
        match = process.extractOne(title, self.choices)
        return match[0] if match else 'Other'

    def _parse_revenue(self, card):
        x = '//dd[contains(@class, "yearly-revenue")]/text()'
        return card.xpath(x).extract_first()

    def _parse_income(self, card):
        x = '//dd[contains(@class, "-yearly-profit")]/text()'
        return card.xpath(x).extract_first()

    def _parse_status(self, card):
        return 'active'


