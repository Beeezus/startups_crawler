from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import pendulum
from airflow.hooks.S3_hook import S3Hook


class GetRunOperator(BaseOperator):
    S3_BUCKET = "airflow-test-347525257716"
    PREFIX = "json_data"
    ATHENA_PREFIX = "output/victor"

    @apply_defaults
    def __init__(self, sites=None, **kwargs):
        self.sites = sites
        super().__init__(**kwargs)

    def execute(self, context):
        run = self.get_max_runs_across_s3()
        context['task_instance'].xcom_push(
            key='run_count',
            value=run + 1
        )

    def get_max_runs_across_s3(self):
        dag_run_list = []
        date = pendulum.now().to_date_string()
        s3 = S3Hook()

        for site in self.sites or []:
            partition = f"site={site}/date={date}"
            files = s3.list_keys(
                bucket_name=self.S3_BUCKET,
                prefix=f'{self.PREFIX}/{partition}'
            )
            file_count = len(files) if files else 0
            dag_run_list.append(file_count)

        return max(dag_run_list)