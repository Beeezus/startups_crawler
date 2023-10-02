from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from scrapy import signals
from twisted.internet import reactor, defer
from scrapy.crawler import Crawler, CrawlerProcess
from scrapy.utils.serialize import ScrapyJSONEncoder
from scrapy.utils.project import get_project_settings


class ReactorControl:

    def __init__(self):
        pass

    def add_crawler(self):
        pass

    def remove_crawler(self):
        pass

scrapy_encoder = ScrapyJSONEncoder()
items = []


class CrawlOperator(BaseOperator):

    @apply_defaults
    def __init__(self, crawler=None, **kwargs):
        super().__init__(**kwargs)
        self.crawler = crawler
        print(self.crawler)

    def collect_items(item, response, spider):
        items.append(scrapy_encoder.encode(item))

    def execute(self, context):
        print(self.crawler)
        pass
