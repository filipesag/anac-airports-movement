import logging
from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType
from include.transform.data_processing import DataProcessor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import traceback
import sys
import os

sys.path.append('/usr/local/airflow/include')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


def list_s3_files_direct(spark, bucket, prefix):
    try:
        # S3 config
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoop_conf.set('fs.s3a.access.key', os.environ.get('AWS_ACCESS_KEY_ID', ''))
        hadoop_conf.set('fs.s3a.secret.key', os.environ.get('AWS_SECRET_ACCESS_KEY', ''))
        hadoop_conf.set('fs.s3a.endpoint', 's3.sa-east-1.amazonaws.com')
        hadoop_conf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        hadoop_conf.set('fs.s3a.connection.ssl.enabled', 'true')
        
        # usando hadoop para obter os arquivos do S3
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
            sc._jvm.java.net.URI.create(f's3a://{bucket}'), 
            hadoop_conf
        )
        
        path = sc._jvm.org.apache.hadoop.fs.Path(f's3a://{bucket}/{prefix}')
        
        if not fs.exists(path):
            logger.warning(f'Path não existe: {path}')
            return []
        
        # arquivos listados
        statuses = fs.listStatus(path)
        files = []
        
        for file_status in statuses:
            if not file_status.isDirectory() and file_status.getPath().getName().endswith('.csv'):
                files.append(file_status.getPath().toString())
        
        logger.info(f'Found {len(files)} CSV file in {path}')
        return files
        
    except Exception as e:
        logger.error(f'Unable to list S3 files: {e}')
        return []
    
def transform_anac_mov_files():
    spark = None
    try:
        logger.info('Starting Anac Files Processing')

        spark = SparkSession.builder \
            .appName('ANAC_Data_Processing') \
            .config('spark.master', 'local[2]') \
            .config('spark.sql.adaptive.enabled', 'true') \
            .getOrCreate()
        
        # S3 config
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoop_conf.set('fs.s3a.access.key', os.environ.get('AWS_ACCESS_KEY_ID', ''))
        hadoop_conf.set('fs.s3a.secret.key', os.environ.get('AWS_SECRET_ACCESS_KEY', ''))
        hadoop_conf.set('fs.s3a.endpoint', 's3.sa-east-1.amazonaws.com')
        hadoop_conf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        hadoop_conf.set('fs.s3a.connection.ssl.enabled', 'true')

        logger.info('✅ SparkSession set')

        processor = DataProcessor(spark)
        bucket = 'anac-mov'


        problematic_file_path = f's3a://{bucket}/bronze/anac_movimentacoes/2022/Movimentacoes_Aeroportuarias_202207.csv'

        anac_mov_columns_default = {
            'ANO': 'ano', 'MES': 'mes', 'NR_AEROPORTO_REFERENCIA': 'aeroporto_ref', 'NR_MOVIMENTO_TIPO': 'tipo_movimento',
            'NR_AERONAVE_MARCAS': 'matricula_aeronave', 'NR_AERONAVE_TIPO': 'aeronave_modelo_icao',
            'NR_AERONAVE_OPERADOR': 'aeronave_operador', 'NR_VOO_OUTRO_AEROPORTO': 'aeroporto_outro',
            'NR_VOO_NUMERO': 'numero_voo', 'NR_SERVICE_TYPE': 'tipo_servico', 'NR_NATUREZA': 'natureza_operacao',
            'DT_PREVISTO': 'data_prevista_movimento', 'HH_PREVISTO': 'hora_prevista_movimento',
            'DT_CALCO': 'data_calco', 'HH_CALCO': 'hora_calco', 'DT_TOQUE': 'data_manobra',
            'HH_TOQUE': 'hora_manobra', 'QT_PAX_LOCAL': 'qtd_pax_local', 'QT_PAX_CONEXAO_DOMESTICO': 'qtd_pax_conexao_domestico',
            'QT_PAX_CONEXAO_INTERNACIONAL': 'qtd_pax_conexao_internacional', 'QT_CORREIO': 'qtd_correio',
            'QT_CARGA': 'qtd_carga'
        } 
        
        anac_mov_fields_default = [
            ('ANO', StringType(), True), ('MES', StringType(), True), ('NR_AEROPORTO_REFERENCIA', StringType(), True),
            ('NR_MOVIMENTO_TIPO', StringType(), True), ('NR_AERONAVE_MARCAS', StringType(), True),
            ('NR_AERONAVE_TIPO', StringType(), True), ('NR_AERONAVE_OPERADOR', StringType(), True),
            ('NR_VOO_OUTRO_AEROPORTO', StringType(), True), ('NR_VOO_NUMERO', StringType(), True),
            ('NR_SERVICE_TYPE', StringType(), True), ('NR_NATUREZA', StringType(), True),
            ('DT_PREVISTO', TimestampType(), True), ('HH_PREVISTO', TimestampType(), True),
            ('DT_CALCO', TimestampType(), True), ('HH_CALCO', TimestampType(), True),
            ('DT_TOQUE', TimestampType(), True), ('HH_TOQUE', TimestampType(), True),
            ('NR_CABECEIRA', StringType(), True), ('NR_BOX', StringType(), True),
            ('NR_PONTE_CONECTOR_REMOTO', StringType(), True), ('NR_TERMINAL', StringType(), True),
            ('QT_PAX_LOCAL', IntegerType(), True), ('QT_PAX_CONEXAO_DOMESTICO', IntegerType(), True),
            ('QT_PAX_CONEXAO_INTERNACIONAL', IntegerType(), True), ('QT_CORREIO', DoubleType(), True),
            ('QT_CARGA', DoubleType(), True)
        ]

        anac_mov_fields_2022 = [
            ('NR_AERONAVE_MARCAS', StringType(), True), ('NR_AERONAVE_TIPO', StringType(), True),
            ('NR_AERONAVE_OPERADOR', StringType(), True), ('NR_VOO_OUTRO_AEROPORTO', StringType(), True),
            ('NR_VOO_NUMERO', StringType(), True), ('DT_PREVISTO', TimestampType(), True),
            ('HH_PREVISTO', TimestampType(), True), ('DT_CALCO', TimestampType(), True),
            ('HH_CALCO', TimestampType(), True), ('DT_TOQUE', TimestampType(), True),
            ('HH_TOQUE', TimestampType(), True), ('NR_CABECEIRA', StringType(), True),
            ('NR_TERMINAL', StringType(), True), ('QT_PAX_LOCAL', IntegerType(), True),
            ('QT_PAX_CONEXAO_DOMESTICO', IntegerType(), True), ('QT_CORREIO', DoubleType(), True),
            ('QT_CARGA', DoubleType(), True), ('ANO', StringType(), True),
            ('MES', StringType(), True), ('NR_AEROPORTO_REFERENCIA', StringType(), True),
            ('NR_MOVIMENTO_TIPO', StringType(), True), ('NR_SERVICE_TYPE', StringType(), True),
            ('NR_NATUREZA', StringType(), True), ('NR_BOX', StringType(), True),
            ('NR_PONTE_CONECTOR_REMOTO', StringType(), True),
            ('QT_PAX_CONEXAO_INTERNACIONAL', IntegerType(), True)
        ]
        
        df_problematic = spark.read.option('header', 'true').option('nullValue', 'null').schema(processor.create_schema(anac_mov_fields_2022)).csv(problematic_file_path, sep=';')
        
        anac_mov_columns_problematic = {
            'NR_AERONAVE_MARCAS': 'matricula_aeronave', 'NR_AERONAVE_TIPO': 'aeronave_modelo_icao',
            'NR_AERONAVE_OPERADOR': 'aeronave_operador', 'NR_VOO_OUTRO_AEROPORTO': 'aeroporto_outro',
            'NR_VOO_NUMERO': 'numero_voo', 'NR_SERVICE_TYPE': 'tipo_servico', 'NR_NATUREZA': 'natureza_operacao',
            'DT_PREVISTO': 'data_prevista_movimento', 'HH_PREVISTO': 'hora_prevista_movimento',
            'DT_CALCO': 'data_calco', 'HH_CALCO': 'hora_calco', 'DT_TOQUE': 'data_manobra',
            'HH_TOQUE': 'hora_manobra', 'QT_PAX_LOCAL': 'qtd_pax_local', 'QT_PAX_CONEXAO_DOMESTICO': 'qtd_pax_conexao_domestico',
            'QT_PAX_CONEXAO_INTERNACIONAL': 'qtd_pax_conexao_internacional', 'QT_CORREIO': 'qtd_correio',
            'QT_CARGA': 'qtd_carga','ANO': 'ano', 'MES': 'mes', 'NR_AEROPORTO_REFERENCIA': 'aeroporto_ref', 'NR_MOVIMENTO_TIPO': 'tipo_movimento'
        }
        df_problematic = processor.select_and_rename_columns(df_problematic, anac_mov_columns_problematic)

        normal_files_paths = []
        for year in ['2019', '2020', '2021', '2022', '2023', '2024']:
            prefix = f'bronze/anac_movimentacoes/{year}/'
            files = list_s3_files_direct(spark, bucket, prefix)
            normal_files = [f for f in files if problematic_file_path not in f]
            normal_files_paths.extend(normal_files)
            logger.info(f'Found {len(normal_files)} normal files for {year}')
        
            
        df_normal = spark.read.option('header', 'true').option('nullValue', 'null').schema(processor.create_schema(anac_mov_fields_default)).csv(normal_files_paths, sep=';')
        
        df_normal = processor.select_and_rename_columns(df_normal, anac_mov_columns_default)

        df_final = df_normal.unionByName(df_problematic)
        
        df_final = df_final.withColumn('mes', F.when(F.col('mes') == 1, 'Janeiro').
                                    when(F.col('mes') == 2, 'Fevereiro').
                                    when(F.col('mes') == 3, 'Março').
                                    when(F.col('mes') == 4, 'Abril').
                                    when(F.col('mes') == 5, 'Maio').
                                    when(F.col('mes') == 6, 'Junho').
                                    when(F.col('mes') == 7, 'Julho').
                                    when(F.col('mes') == 8, 'Agosto').
                                    when(F.col('mes') == 9, 'Setembro').
                                    when(F.col('mes') == 10, 'Outubro').
                                    when(F.col('mes') == 11, 'Novembro').
                                    when(F.col('mes') == 12, 'Dezembro').
                                    otherwise('Mês não informado'))

        df_final = df_final.withColumn('natureza_operacao', F.when(F.col('natureza_operacao')=='D', 'Doméstico').
                                    when(F.col('natureza_operacao')=='I', 'Internacional').
                                    otherwise('Não informado'))
        
        
        w = Window.partitionBy(['numero_voo','data_prevista_movimento','data_calco','data_manobra','hora_prevista_movimento','hora_calco','hora_manobra']).orderBy('data_calco') 
        df_final = df_final.withColumn('rn', F.row_number().over(w)).filter('rn = 1').drop('rn')

        df_final = df_final.withColumn('data_prevista_movimento', F.to_date('data_prevista_movimento')) \
        .withColumn('data_manobra', F.to_date('data_manobra')) \
        .withColumn('hora_prevista_movimento', F.date_format('hora_prevista_movimento', 'HH:mm')) \
        .withColumn('hora_calco', F.date_format('hora_calco', 'HH:mm')) \
        .withColumn('hora_manobra', F.date_format('hora_manobra', 'HH:mm')) 

        output_path = f's3a://{bucket}/silver'
        df_final.write.mode('overwrite').partitionBy('ano', 'mes').parquet(f'{output_path}/anac_movimentacoes/')

        logging.info(f'File was processed and saved in silver layer - Partitioned by year/month')

    except Exception as e:
        logger.error(f'Processing has failed: {e}')
        logger.error(traceback.format_exc())
        raise
    finally:
        if spark is not None:
            spark.stop()
            logger.info('SparkSession finished')


def main():
    transform_anac_mov_files()

if __name__ == '__main__':
    main()