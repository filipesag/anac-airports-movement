import logging
from pyspark.sql.types import StringType,TimestampType
from include.transform.data_processing import DataProcessor
from pyspark.sql import SparkSession
from airflow.decorators import task
from airflow.models import Variable
import traceback
import sys
import os

sys.path.append('/usr/local/airflow/include')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

@task(task_id='transform_airports_files')
def transform_airports_file():

    aws_access_key = Variable.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = Variable.get('AWS_SECRET_ACCESS_KEY')

    spark = None
    try:

        logger.info('Starting Airport Files Processing')

        spark = (
            SparkSession.builder
                .appName('ANAC_Data_Processing')
                .config('spark.jars', os.getenv('SPARK_JARS'))  
                .config('spark.hadoop.fs.s3a.access.key', aws_access_key)
                .config('spark.hadoop.fs.s3a.secret.key', aws_secret_key)
                .config('spark.hadoop.fs.s3a.endpoint', 's3.sa-east-1.amazonaws.com')
                .config('spark.local.dir', '/tmp/spark')
                .config('spark.driver.memory', '4g')
                .config('spark.executor.memory', '4g')
                .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
                .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'true')
                .getOrCreate()
        )

        logger.info('✅ SparkSession set')

        processor = DataProcessor(spark)
        bucket = 'anac-mov'

        airpots_columns = {'ident':'aeroporto_icao',
                        'type':'tipo_aeroporto',
                        'name':'nome_aeroporto',
                        'continent':'continente',
                        'iso_country':'pais_iso',
                        'municipality':'cidade'}

        airports_fields_default = [
            ('ident', StringType(), True), ('type', StringType(), True), ('name', StringType(), True),
            ('elevation_ft', StringType(), True), ('continent', StringType(), True),
            ('iso_country', StringType(), True), ('iso_region', StringType(), True),
            ('municipality', StringType(), True), ('icao_code', StringType(), True),
            ('iata_code', StringType(), True), ('gps_code', StringType(), True),
            ('local_code', TimestampType(), True), ('coordinates', TimestampType(), True)
        ]

        airports_schema = processor.create_schema(airports_fields_default)
        input_path = f's3a://{bucket}/bronze/icao_aeroportos/airport-codes.csv'

        airports_df = processor.read_file(airports_schema,input_path,sep=',')
        airports_df = processor.select_and_rename_columns(airports_df,airpots_columns)

        airports_df = airports_df.dropDuplicates()

        airports_df = airports_df.coalesce(1)

        output_path = f's3a://{bucket}/silver/icao_aeroportos/'
        airports_df.write \
            .mode('overwrite') \
            .parquet(output_path)
        
        logger.info(f'File processed and saved in silver layer')

    except Exception as e:
        logger.error(f'Icao Airports Files Processing failed: {e}')
        logger.error(traceback.format_exc())
        raise
    finally:
        if spark is not None:
            spark.stop()
            logger.info('SparkSession finished')


def main():
    transform_airports_file()

if __name__ == '__main__':
    main()