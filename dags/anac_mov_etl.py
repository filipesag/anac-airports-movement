from datetime import datetime
from pyspark.sql import SparkSession
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from src.anac_web_scraping import scrape_iata_service_types
from src.data_processing import DataProcessor
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from airflow.models import Variable
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

aws_access_key = Variable.get("AWS_ACCESS_KEY_ID")
aws_secret_key = Variable.get("AWS_SECRET_ACCESS_KEY")

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
        logging.info("Iata Service file saved in bronze/anac_scraping")

    @task(task_id="transform_anac_mov_files")
    def transform_anac_mov_files():

        spark = (
            SparkSession.builder
            .appName("ANAC_Data_Processing")
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            .config("spark.local.dir", "/tmp/spark")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
            .getOrCreate()
        )
        processor = DataProcessor(spark)

        anac_mov_columns = {
            "ANO": "ano",
            "MES": "mes",
            "NR_AEROPORTO_REFERENCIA": "aeroporto_ref",
            "NR_MOVIMENTO_TIPO": "tipo_movimento",
            "NR_AERONAVE_MARCAS": "matricula_aeronave",
            "NR_AERONAVE_TIPO": "aeronave_modelo_icao",
            "NR_AERONAVE_OPERADOR": "aeronave_operador",
            "NR_VOO_OUTRO_AEROPORTO": "aeroporto_outro",
            "NR_VOO_NUMERO": "numero_voo",
            "NR_SERVICE_TYPE": "tipo_servico",
            "NR_NATUREZA": "natureza_operacao",
            "DT_PREVISTO": "data_prevista_movimento",
            "HH_PREVISTO": "hora_prevista_movimento",
            "DT_CALCO": "data_calco",
            "HH_CALCO": "hora_calco",
            "DT_TOQUE": "data_manobra",
            "HH_TOQUE": "hora_manobra",
            "QT_PAX_LOCAL": "qtd_pax_local",
            "QT_PAX_CONEXAO_DOMESTICO": "qtd_pax_conexao_domestico",
            "QT_PAX_CONEXAO_INTERNACIONAL": "qtd_pax_conexao_internacional",
            "QT_CORREIO": "qtd_correio",
            "QT_CARGA": "qtd_carga"
        } 

        
        anac_mov_fields = [
            ("ANO", StringType(), True),
            ("MES", StringType(), True),
            ("NR_AEROPORTO_REFERENCIA", StringType(), True),
            ("NR_MOVIMENTO_TIPO", StringType(), True),
            ("NR_AERONAVE_MARCAS", StringType(), True),
            ("NR_AERONAVE_TIPO", StringType(), True),
            ("NR_AERONAVE_OPERADOR", StringType(), True),
            ("NR_VOO_OUTRO_AEROPORTO", StringType(), True),
            ("NR_VOO_NUMERO", StringType(), True),
            ("NR_SERVICE_TYPE", StringType(), True),
            ("NR_NATUREZA", StringType(), True),
            ("DT_PREVISTO", TimestampType(), True),
            ("HH_PREVISTO", TimestampType(), True),
            ("DT_CALCO", TimestampType(), True),
            ("HH_CALCO", TimestampType(), True),
            ("DT_TOQUE", TimestampType(), True),
            ("HH_TOQUE", TimestampType(), True),
            ("NR_CABECEIRA", StringType(), True),
            ("NR_BOX", StringType(), True),
            ("NR_PONTE_CONECTOR_REMOTO", StringType(), True),
            ("NR_TERMINAL", StringType(), True),
            ("QT_PAX_LOCAL", IntegerType(), True),
            ("QT_PAX_CONEXAO_DOMESTICO", IntegerType(), True),
            ("QT_PAX_CONEXAO_INTERNACIONAL", IntegerType(), True),
            ("QT_CORREIO", DoubleType(), True),
            ("QT_CARGA", DoubleType(), True)
        ]
        schema = processor.create_schema(anac_mov_fields)
        
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket = 'anac-mov'
        years = ['2019','2020','2021','2022','2023','2024']
        months = ['jan','fev','mar','abr','maio','jun','julho','ago','set','out','nov','dez']
        index = 0

        for year in years:
  
            prefix = f'bronze/anac_movimentacoes/{year}/'  
            anac_file_list = s3_hook.list_keys(bucket_name='anac-mov', prefix=prefix)

            csv_files = [key for key in anac_file_list if key and key.endswith('.csv')]
            for file in csv_files:
                s3_path = f"s3a://{bucket}/{file}"
                
                df = processor.read_file(path=s3_path,schema=schema,sep=';')
                df = processor.select_and_rename_columns(df,anac_mov_columns)
                output_path = f"s3a://{bucket}/silver/anac_movimentacoes/{year}/anac_movimentacoes_{months[index]}"

                df.write.mode("overwrite").parquet(output_path)
                logging.info(f"File anac_movimentacoes_{months[index]}.parquet save in silver layer")
                index += 1
        spark.stop()

    @task(task_id="transform_iata_service_file")
    def transform_iata_service_file():

        spark = (
            SparkSession.builder
            .appName("IATA_Data_Processing")
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            .config("spark.local.dir", "/tmp/spark")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
            .getOrCreate()
        )
        
        processor = DataProcessor(spark)

        iata_columns = {"​Service Type Code":"cod_tipo_servico",
                        "​Type of Operation":"tipo_servico_operacao",
                        "​Service Type Description​​":"tipo_servico_desc"}

        iata_fields = [
            ("​Service Type Code", StringType(), True),
            ("​Application", StringType(), True),
            ("​Type of Operation", StringType(), True),
            ("​Service Type Description​​", StringType(), True)
        ]
        schema_service = processor.create_schema(iata_fields)

        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket = 'anac-mov'
        prefix = f'bronze/anac_scraping/'  
        
        anac_scrap_file = s3_hook.list_keys(bucket_name='anac-mov', prefix=prefix)
        service_file = [file for file in anac_scrap_file if not file.endswith("/")]
           
        s3_path = f"s3a://{bucket}/{service_file[0]}"
  
        df_service_type = processor.read_file(path=s3_path,schema=schema_service,sep=',')
        df_service_type = processor.select_and_rename_columns(df_service_type,iata_columns)
        output_path_iata = f"s3a://{bucket}/silver/anac_scraping/"
        df_service_type.write.mode("overwrite").parquet(output_path_iata)
        logging.info(f"File iata_service_type.parquet save in silver layer")

        spark.stop()


    scraping_and_save_to_s3() >> [transform_anac_mov_files(),transform_iata_service_file()]

anac_etl()
