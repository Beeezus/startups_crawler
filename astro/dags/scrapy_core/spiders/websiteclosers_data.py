# -*- coding: utf-8 -*-
import json

import scrapy
from fuzzywuzzy import process
from scrapy import FormRequest
from scrapy.selector import Selector
from scrapy_core.items import DealsItem


class WebsiteclosersDataSpider(scrapy.Spider):
    name = 'websiteclosers_data'
    allowed_domains = ['websiteclosers.com']
    PAYLOAD = {
        'taxonomy': 'businesses-category',
        'slug': 'businesses-for-sale',
        'page': '1',
    }
    choices = [
        'saas', 'subscription', 'display advertising',
        'AdSense', 'lead gen', 'affiliate', 'blog',
        'forum', 'review', 'Ecommerce'
    ]

    def __init__(self, date, run_count=0):
        self.run_count = run_count
        self.date = date
        super(WebsiteclosersDataSpider, self).__init__()

    def start_requests(self):
        yield FormRequest(
            method='POST',
            url='https://www.websiteclosers.com/wp-admin/admin-ajax.php?action=ajax_req',
            formdata=self.PAYLOAD,
            callback=self.parse
        )

    def parse(self, response):
        page = response.meta.get('page') or 1
        data = self._get_html_data(response)
        cards = self.extract_cards(data)

        if not cards:
            return

        for card in cards or []:
            item = DealsItem(
                title=self._parse_title(card),
                url=self._parse_url(card),
                price=self._parse_price(card),
                type=self._parse_type(card),
                revenue=None,
                income=self._parse_income(card),
                status=self._parse_status(card),
                id=self._parse_id(card),
                site='wcs',
                run=self.run_count,
                date=self.date
            )
            yield item

        page += 1
        self.PAYLOAD['page'] = str(page)
        yield FormRequest(
            method='POST',
            url='https://www.websiteclosers.com/wp-admin/admin-ajax.php?action=ajax_req',
            formdata=self.PAYLOAD,
            callback=self.parse,
            meta={'page': page}
        )

    def extract_cards(self, data):
        selector = Selector(text=data)
        return selector.xpath('//div[@class="post_item"]')

    def _parse_id(self, card):
        url = self._parse_url(card)
        if not  url:
            return None

        url_list = url.split('/')
        if not url_list:
            return None

        return url_list[-1]

    def _get_html_data(self, response):
        data = json.loads(response.body)
        content = data.get('content')
        return content if content else None

    def _parse_title(self, card):
        x = './/a[@class="post_title"]/text()'
        return card.xpath(x).extract_first()

    def _parse_price(self, card):
        x = './/div[@class="asking_price"]/strong/text()'
        return card.xpath(x).extract_first()

    def _parse_url(self, card):
        x = './/a[@class="post_title"]/@href'
        return card.xpath(x).extract_first()

    def _parse_type(self, card):
        title = self._parse_title(card)
        match = process.extractOne(title, self.choices)
        return match[0] if match else 'Other'

    def _parse_income(self, card):
        x = './/div[@class="cash_flow"]/strong/text()'
        return card.xpath(x).extract_first()

    def _parse_status(self, card):
        status_data = {
            'available': self._get_available_status(card),
            'pending': self._get_pending_status(card),
            'sold': self._get_sold_status(card)
        }
        status =[
            status for status, data_exists
            in status_data.items() if data_exists
        ]
        return status[0] if status else None

    def _get_pending_status(self, card):
        x = './/span[@class="badge pending"]'
        return card.xpath(x).extract_first()

    def _get_sold_status(self, card):
        x = './/img[@class="sold_flag"]'
        return card.xpath(x).extract_first()

    def _get_available_status(self, card):
        x = './/span[@class="badge businesses-for-sale"]/text()'
        return card.xpath(x).extract_first()
