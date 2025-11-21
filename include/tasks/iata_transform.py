from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from airflow.decorators import task
from airflow.models import Variable
import logging
import traceback
import sys
import os

sys.path.append('/usr/local/airflow/include')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

@task(task_id='transform_iata_files')
def transform_iata_service_file():

    aws_access_key = Variable.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = Variable.get('AWS_SECRET_ACCESS_KEY')

    spark = None
    try:
        logger.info('Starting IATA Files Processing')

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
        
        s3_path = f's3a://anac-mov/bronze/anac_scraping/'

        df_service_type = spark.read.option('header', 'true').csv(s3_path, sep=',')

        iata_columns = {
            'Service Type Code': 'cod_tipo_servico',
            'Application': 'aplicacao_servico',
            'Type of Operation': 'tipo_servico_operacao',
            'Service Type Description': 'tipo_servico_desc'
        }

        df_service_type = df_service_type.select(
            *[F.col(c).alias(iata_columns.get(c, c)) for c in iata_columns.keys()]
        )

        df_service_type = df_service_type.withColumn('cod_tipo_servico', F.regexp_replace(F.col('cod_tipo_servico'), u'\u200b', ''))
        df_service_type = df_service_type.withColumn('cod_tipo_servico', F.trim(F.col('cod_tipo_servico')))

        df_service_type = df_service_type.dropDuplicates()

        df_service_type = df_service_type.coalesce(1)

        output_path_iata = f's3a://anac-mov/silver/anac_scraping/'
        df_service_type.write \
            .mode('overwrite') \
            .parquet(output_path_iata)

        logging.info(f'File processed and saved in silver layer')

    except Exception as e:
        logger.error(f'Iata Service File Processing failed: {e}',exc_info=True)
        logger.error(traceback.format_exc())
        raise
    finally:
        if spark is not None:
            spark.stop()
            logger.info('SparkSession finished')

def main():
    transform_iata_service_file()

if __name__ == '__main__':
    main()