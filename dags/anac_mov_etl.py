from datetime import datetime
from airflow.decorators import dag
from include.tasks.airports_transform import transform_airports_file
from include.tasks.anac_scraping import scraping_and_save_to_s3
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
import logging

from include.tasks.iata_transform import transform_iata_service_file


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

@dag(
    dag_id='anac_etl',
    start_date=datetime(2025, 8, 19),
    schedule='@once',
    catchup=False,
    owner_links={'Linkedin':'https://www.linkedin.com/in/filipe-aguiar-421269b5/'},
    tags=['anac', 'etl','aws']
)
def anac_etl():

    aws_access_key = Variable.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = Variable.get('AWS_SECRET_ACCESS_KEY')

    scraping = scraping_and_save_to_s3()
    iata = transform_iata_service_file()
    airports = transform_airports_file()

    anac = SparkSubmitOperator(
        task_id="transform_anac_mov_files",
        application="/usr/local/airflow/include/tasks/anac_mov_transform.py",
        conn_id="spark_default",
        env_vars={
            'AWS_ACCESS_KEY_ID': aws_access_key,
            'AWS_SECRET_ACCESS_KEY': aws_secret_key
        },
        conf={ 
            "spark.driver.memory": "6g",
            "spark.executor.memory": "8g",
        },
        jars="/opt/spark/jars/hadoop-aws-3.3.2.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars/wildfly-openssl-1.0.7.Final.jar", 
        verbose=True
    )

    enrich_data = SparkSubmitOperator(
        task_id="enrich_data",
        application="/usr/local/airflow/include/tasks/enrich.py",
        conn_id="spark_default",
        env_vars={
            'AWS_ACCESS_KEY_ID': aws_access_key,
            'AWS_SECRET_ACCESS_KEY': aws_secret_key
        },
        conf={
            "spark.driver.memory": "6g",
            "spark.executor.memory": "8g",
        },
        jars="/opt/spark/jars/hadoop-aws-3.3.2.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars/wildfly-openssl-1.0.7.Final.jar", 
        verbose=False
    )

    
    transform_tasks = [
        anac,
        airports,
        iata
    ]
    enrich_data 

    scraping >> transform_tasks
    transform_tasks >> enrich_data

anac_etl()
