from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from src.anac_web_scraping import scrape_iata_service_types

@dag(
    dag_id="anac_etl",
    start_date=datetime(2025, 8, 19),
    schedule="@once",
    catchup=False,
    owner_links={'Linkedin':'https://www.linkedin.com/in/filipe-aguiar-421269b5/'},
    tags=["anac", "etl","aws","snowflake"]
)
def anac_etl():

    @task(task_id="anac_scraping", retries=3)
    def scraping_and_save_to_s3():
        service_type = scrape_iata_service_types()

        s3_hook = S3Hook(aws_conn_id='aws_default')

        bucket_name = 'anac-mov'
        key = 'bronze/anac_scraping/iata_service_type.csv'

        s3_hook.load_string(
            string_data=service_type,
            bucket_name=bucket_name,
            key=key,
            replace=True
        )
    
    scraping_and_save_to_s3()

anac_etl()
