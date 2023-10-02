import scrapy
from price_parser import Price


def serialize_price(value):
    if not value:
        return 0
    price_value = Price.fromstring(value)
    return price_value.amount or 0

def serializer_status(value):
    return value or 'N/A'

class DealsItem(scrapy.Item):
    title = scrapy.Field()
    url = scrapy.Field()
    status = scrapy.Field(serializer=serializer_status)
    type = scrapy.Field()
    price = scrapy.Field(serializer=serialize_price)
    revenue = scrapy.Field(serializer=serialize_price)
    income = scrapy.Field(serializer=serialize_price)
    site = scrapy.Field()
    date = scrapy.Field()
    run = scrapy.Field()
    id = scrapy.Field()