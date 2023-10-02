from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from run_counter_operator import GetRunOperator
from scrapy import signals
from scrapy.crawler import Crawler, CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.utils.serialize import ScrapyJSONEncoder
from scrapy_core.spiders.abb_data import AbbDataSpider
from scrapy_core.spiders.fei_data import FeiDataSpider
from scrapy_core.spiders.qlb_data import QlbDataSpider
from scrapy_core.spiders.latonas_data import LatonasDataSpider
from scrapy_core.spiders.websiteclosers_data import WebsiteclosersDataSpider
from upload_to_s3_data import UploadToS3Operator
import pendulum


scrapy_encoder = ScrapyJSONEncoder()
items = []

def collect_items(item, response, spider):
    items.append(scrapy_encoder.encode(item))

def start_crawl(crawler_name, **context):
    run_count = context['ti'].xcom_pull(key='run_count')
    date = pendulum.now().to_date_string()
    spider = Crawler(crawler_name)
    spider.signals.connect(collect_items, signals.item_scraped)

    process = CrawlerProcess(get_project_settings())
    process.crawl(spider, {'run_count': run_count, 'date': date})
    process.start()
    return items

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    's3_bucket': "airflow-test-347525257716",
    'prefix' : 'json_data'
}

with DAG('deals_crawl_dag',
         start_date=datetime(2019, 1, 1),
         max_active_runs=1,
         schedule_interval=None,
         default_args=default_args,
         ) as dag:

    crawlers = {
        'qlb': QlbDataSpider,
        'abb': AbbDataSpider,
        'fei': FeiDataSpider,
        'lat': LatonasDataSpider,
        'wbc': WebsiteclosersDataSpider
    }

    start = DummyOperator(
        task_id='start'
    )

    get_run = GetRunOperator(
        task_id='get_run_count',
        provide_context=True,
        sites=crawlers.keys()
    )

    for crawler_name in crawlers or {}:
        crawl_site = PythonOperator(
            task_id=f'{crawler_name}_crawl',
            python_callable=start_crawl,
            provide_context=True,
            op_kwargs={'crawler_name': crawlers.get(crawler_name)}
        )

        upload_data_to_s3 = UploadToS3Operator(
            task_id=f'upload_{crawler_name}_to_s3',
            provide_context=True,
            crawler_name=crawler_name,
            trigger_rule='all_success'
        )

        end = DummyOperator(
            task_id='end'
        )

        start >> get_run >> crawl_site >> upload_data_to_s3 >> end
