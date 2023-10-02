import re

import scrapy
from scrapy_core.items import DealsItem
from scrapy import Request
from w3lib import html


class LatonasItem(DealsItem):
    description = scrapy.Field()
    broker_name = scrapy.Field()


class LatonasDataSpider(scrapy.Spider):
    name = 'latonas_data'
    allowed_domains = ['www.latonas.com', 'latonas.com']
    start_urls = [(
        'https://latonas.com/listings/?page=ALL&result_sorting_quantity=1000'
        '&result_sorting_order=&broker=any&price_range=any&revenue_range=any'
        '&searchTags=&unique_range=any&profit_range=any'
    )]

    def __init__(self, date=None, run_count=0):
        self.run_count = run_count
        self.date = date
        super(LatonasDataSpider, self).__init__()

    def parse(self, response):
        listing_data = self._get_url_and_type(response)
        for listing in listing_data or {}:
            yield Request(
                url=listing.get('url'),
                callback=self.listing_callback,
                meta={'type': listing.get('type')}
            )

    def listing_callback(self, response):
        listing_type = response.meta.get('type')
        item = LatonasItem(
            title=self._parse_title(response),
            url=response.url,
            price=self._parse_price(response),
            type=listing_type,
            revenue=self._parse_revenue(response),
            income=self._parse_income(response),
            status=self._parse_status(response),
            broker_name=self._parse_broker_name(response),
            description=self._parse_description(response),
            id=self._parse_listing_id(response),
            run=self.run_count,
            date=self.date,
            site='lat'
        )
        print(item)
        yield item

    def _parse_listing_id(self, response):
        found = re.search(r'-(\d+)\/', response.url)
        if found:
            return found.group(1)

    def _get_url_and_type(self, response):
        listigns_data = []
        path = '//div[contains(@class, "ct-itemProducts")]'
        listings = response.xpath(path)
        for listing in listings:
            url = self._parse_listing_url(listing, response)
            type = self._parse_type(listing)
            listigns_data.append({
                'url': url,
                "type": type
            })
        return listigns_data

    def _parse_listing_url(self, listing, response):
        url = listing.xpath('./a/@href').extract_first()
        if url:
            return response.urljoin(url)

    def _parse_broker_name(self, response):
        x = '//h5[@class="ct-fw-600"]/text()'
        return response.xpath(x).extract_first()

    def _parse_description(self, response):
        x = '//p[@class="ct-u-marginBottom20"]/following-sibling::p/text()'
        return response.xpath(x).extract_first()

    def _parse_title(self, card):
        x = 'normalize-space(//h2/text())'
        return card.xpath(x).extract_first()

    def _parse_price(self, response):
        x = '//div[./p/span/text()="Asking Price"]/span/@data-ct-to'
        return response.xpath(x).extract_first()

    def _parse_type(self, listing):
        x = (
            './/div[@class="ct-product--description"'
            ' and contains(strong/text(),"Revenue Sources")]'
            '/text()'
        )
        type_elements = listing.xpath(x).extract()
        if not type_elements:
            return ""
        return html.replace_escape_chars(type_elements[-1]).strip()

    def _parse_revenue(self, response):
        x = '//div[./p/span/text()="Annual Revenue"]/span/@data-ct-to'
        return response.xpath(x).extract_first()

    def _parse_income(self, response):
        x = '//div[./p/span/text()="Annual Profit"]/span/@data-ct-to'
        return response.xpath(x).extract_first()

    def _parse_status(self, response):
        under_contract = response.xpath('//span[contains(@class, "under-contract")]')
        return 'under_conract' if under_contract else 'active'
