from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import pendulum
from airflow.hooks.S3_hook import S3Hook
import json
import io


class UploadToS3Operator(BaseOperator):
    S3_BUCKET = "airflow-test-347525257716"
    PREFIX = "json_data"
    ATHENA_PREFIX = "output/victor"

    @apply_defaults
    def __init__(self, crawler_name=None, **kwargs):
        super().__init__(**kwargs)
        self.crawler_name = crawler_name

    def execute(self, context):
        run_count = context['ti'].xcom_pull(key='run_count')
        data = context['ti'].xcom_pull(task_ids=f'{self.crawler_name}_crawl')

        date = pendulum.now().to_date_string()
        partition = f"site={self.crawler_name}/date={date}"
        key = f'{self.PREFIX}/{partition}/{run_count}.json'
        data = '\n'.join(data)
        file = json.dumps(data)
        file_obj = io.BytesIO(file.encode('utf-8'))
        s3 = S3Hook()
        s3.load_file_obj(
                file_obj=file_obj,
                key=key,
                bucket_name=self.S3_BUCKET,
            )
