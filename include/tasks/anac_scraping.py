import logging
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.scrap.anac_web_scraping import scrape_iata_service_types


@task(task_id='anac_scraping')
def scraping_and_save_to_s3():

    try:
        logging.info('Starting Anac Scraping to collect Services Types Data')
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
        logging.info('Iata Service file saved in bronze/anac_scraping')
    except Exception as e:
        logging.error(f'Anac Scraping failed: {e}')