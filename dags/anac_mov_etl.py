from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from src.anac_web_scraping import scrape_iata_service_types
from src.data_processing import DataProcessor
from src.data_enriching import DataEnriching
from src.utils import create_spark
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast
from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType
from airflow.models import Variable
import logging


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

    @task(task_id='transform_anac_mov_files')
    def transform_anac_mov_files():

        try:
            logging.info('Starting Anac Files Processing')

            spark = create_spark() 
            processor = DataProcessor(spark)

            s3_hook = S3Hook(aws_conn_id='aws_default')
            bucket = 'anac-mov'
            
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
            
            problematic_file_path = f's3a://{bucket}/bronze/anac_movimentacoes/2022/Movimentacoes_Aeroportuarias_202207.csv'
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
            
            anac_file_list = []
            for year in ['2019', '2020', '2021', '2022', '2023', '2024']:
                prefix = f'bronze/anac_movimentacoes/{year}/'
                files_in_year = s3_hook.list_keys(bucket_name='anac-mov', prefix=prefix)
                anac_file_list.extend([f's3a://{bucket}/{key}' for key in files_in_year if key and key.endswith('.csv')])
                
            normal_files = [f for f in anac_file_list if f != problematic_file_path]
            df_normal = spark.read.option('header', 'true').option('nullValue', 'null').schema(processor.create_schema(anac_mov_fields_default)).csv(normal_files, sep=';')
            
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
    
            logging.info(f'File processed and saved in silver layer - Partitioned by year/month')

        except Exception as e:
            logging.error(f'Anac Files Processing failed: {e}')
        finally:
            spark.stop()


    @task(task_id='transform_iata_service_file')
    def transform_iata_service_file():

        try:
            logging.info('Starting Iata Service File Processing')

            spark = create_spark() 

            s3_hook = S3Hook(aws_conn_id='aws_default')
            bucket = 'anac-mov'
            prefix = 'bronze/anac_scraping/' 

            anac_scrap_file = s3_hook.list_keys(bucket_name='anac-mov', prefix=prefix)
            service_file = [file for file in anac_scrap_file if not file.endswith('/')]
                
            s3_path = f's3a://{bucket}/{service_file[0]}'

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

            output_path_iata = f's3a://{bucket}/silver/anac_scraping/'
            df_service_type.write \
                .mode('overwrite') \
                .parquet(output_path_iata)

            logging.info(f'File processed and saved in silver layer')

        except Exception as e:
            logging.error(f'Iata Service File Processing failed: {e}')
        finally:
            spark.stop()

   
    @task(task_id='transform_airports_file')
    def transform_airports_file():

        try:

            logging.info('Starting Icao Airports Files Processing')

            bucket = 'anac-mov'

            spark = create_spark() 
            processor = DataProcessor(spark)

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

            output_path = f's3a://{bucket}/silver/icao_aeroportos/'
            airports_df.write \
                .mode('overwrite') \
                .parquet(output_path)
            
            logging.info(f'File processed and saved in silver layer')

        except Exception as e:
            logging.error(f'Icao Airports Files Processing failed: {e}')
        finally:
            spark.stop()

    @task(task_id='enrich_data')
    def enrich_anac_mov_files():

        try:
            logging.info('Starting Anac Files Enriching')

            spark = create_spark() 
            processor = DataProcessor(spark)
            enrich = DataEnriching(spark)

            spark.conf.set('spark.sql.legacy.parquet.int96RebaseModeInWrite', 'LEGACY')
            
            bucket = 'anac-mov'

            df_anac = spark.read.parquet(f's3a://{bucket}/silver/anac_movimentacoes/')
            df_iata = spark.read.parquet(f's3a://{bucket}/silver/anac_scraping/')
            df_airports = spark.read.parquet(f's3a://{bucket}/silver/icao_aeroportos/')
            
            df_anac = processor.clean_string_columns(df_anac)
            df_iata = processor.clean_string_columns(df_iata)
            df_airports = processor.clean_string_columns(df_airports)


            df_enriched = df_anac.join(
                broadcast(df_iata),
                on=(F.col('tipo_servico') == F.col('cod_tipo_servico')),
                how='left'
            )

            #enrich data with flags
            df_enriched = enrich.add_day_column(df_enriched)
            df_enriched = enrich.add_flag_covid(df_enriched)
            df_enriched = enrich.add_flag_delay(df_enriched)

            df_enriched = enrich.set_airports(df_enriched)

            df_airports_partida = df_airports.selectExpr(
                'aeroporto_icao as aeroporto_partida_icao',
                'tipo_aeroporto as tipo_aero_partida',
                'nome_aeroporto as nome_aeroporto_partida',
                'continente as continente_partida',
                'pais_iso as pais_partida',
                'cidade as cidade_partida'
            )

            df_airports_chegada = df_airports.selectExpr(
                'aeroporto_icao as aeroporto_chegada_icao',
                'tipo_aeroporto as tipo_aero_chegada',
                'nome_aeroporto as nome_aeroporto_chegada',
                'continente as continente_chegada',
                'pais_iso as pais_chegada',
                'cidade as cidade_chegada'
            )

            df_enriched = df_enriched \
                .join(df_airports_partida, df_enriched['aeroporto_partida'] == F.col('aeroporto_partida_icao'), 'left') \
                .join(df_airports_chegada, df_enriched['aeroporto_chegada'] == F.col('aeroporto_chegada_icao'), 'left')
            
            selected_columns = ['numero_voo','qtd_pax_local','qtd_pax_conexao_domestico','qtd_pax_conexao_internacional', 
                                'qtd_correio','qtd_carga','pandemia_decreto','atraso','matricula_aeronave','aeronave_modelo_icao','aeronave_operador', 
                                'natureza_operacao','data_prevista_movimento', 'hora_prevista_movimento', 'data_calco', 
                                'hora_calco', 'data_manobra','hora_manobra','ano', 'mes', 'dia_semana' ,'cod_tipo_servico', 'aplicacao_servico', 'tipo_servico_operacao', 'tipo_servico_desc', 
                                'aeroporto_partida', 'tipo_aero_partida', 'nome_aeroporto_partida', 'continente_partida', 'pais_partida', 'cidade_partida', 'aeroporto_chegada', 
                                'tipo_aero_chegada', 'nome_aeroporto_chegada', 'continente_chegada', 'pais_chegada', 'cidade_chegada']

            df_enriched_reordered = df_enriched.select(*selected_columns)

            df_enriched_reordered = df_enriched_reordered.withColumn(
                'tipo_aero_partida',
                F.when(F.col('tipo_aero_partida') == 'small_airport', 'Pequeno')
                .when(F.col('tipo_aero_partida') == 'medium_airport', 'Médio')
                .when(F.col('tipo_aero_partida') == 'large_airport', 'Grande')
                .otherwise('Não Informado')
            )

            df_enriched_reordered = df_enriched_reordered.withColumn(
                'tipo_aero_chegada',
                F.when(F.col('tipo_aero_chegada') == 'small_airport', 'Pequeno')
                .when(F.col('tipo_aero_chegada') == 'medium_airport', 'Médio')
                .when(F.col('tipo_aero_chegada') == 'large_airport', 'Grande')
                .otherwise('Não Informado')
            )

            df_enriched_reordered = processor.replace_null_values(df_enriched_reordered, ['ano'], 'Ano não informado')
            df_enriched_reordered = processor.replace_null_values(df_enriched_reordered, ['aeroporto_chegada','aeroporto_partida'], 'Aeroporto não informado')
            df_enriched_reordered = processor.replace_null_values(df_enriched_reordered, ['matricula_aeronave'], 'Matrícula não informada')
            df_enriched_reordered = processor.replace_null_values(df_enriched_reordered, ['aeronave_modelo_icao'], 'Modelo não informado')
            df_enriched_reordered = processor.replace_null_values(df_enriched_reordered, ['aeronave_operador'], 'Operador não informado')
            df_enriched_reordered = processor.replace_null_values(df_enriched_reordered, ['numero_voo'], 'Número de voo não informado')
            df_enriched_reordered = processor.replace_null_values(df_enriched_reordered, ['cod_tipo_servico', 'aplicacao_servico', 'tipo_servico_operacao', 'tipo_servico_desc'], 'Serviço não informado')
            df_enriched_reordered = processor.replace_null_values(df_enriched_reordered, ['natureza_operacao'], 'Tipo de operação não informada')
            df_enriched_reordered = processor.replace_null_values(df_enriched_reordered, ['data_prevista_movimento','data_calco','data_manobra'],  F.lit('1900-01-01').cast('date'))
            df_enriched_reordered = processor.replace_null_values(df_enriched_reordered, ['hora_prevista_movimento','hora_calco','hora_manobra'], 'Hora não informada')
            df_enriched_reordered = processor.replace_null_values(df_enriched_reordered, ['numero_voo','qtd_pax_local','qtd_pax_conexao_domestico','qtd_pax_conexao_internacional', 
                                'qtd_correio','qtd_carga'], -1)
            df_enriched_reordered = processor.replace_null_values(df_enriched_reordered, ['nome_aeroporto_partida', 'continente_partida',
                                                                                        'pais_partida', 'cidade_partida',
                                                                                        'tipo_aero_chegada','nome_aeroporto_chegada', 'continente_chegada', 
                                                                                        'pais_chegada', 'cidade_chegada'], 'Não Informado')

            df_enriched_reordered = df_enriched_reordered.withColumn('voo_id', F.monotonically_increasing_id()) \
            .withColumn('tempo_id', F.monotonically_increasing_id()) \
            .withColumn('partida_id', F.monotonically_increasing_id()) \
            .withColumn('destino_id', F.monotonically_increasing_id()) \
            .withColumn('servico_id', F.monotonically_increasing_id()) \
            .withColumn('aeronave_id', F.monotonically_increasing_id())

            # DIMENSAO TEMPO
            dim_tempo = df_enriched_reordered.select('tempo_id','data_manobra', 'ano', 'mes', 'dia_semana')
            dim_tempo = dim_tempo.dropDuplicates(['data_manobra', 'ano', 'mes', 'dia_semana'])

            # DIMENSAO PARTIDA
            dim_partida = df_enriched_reordered.select('partida_id','aeroporto_partida', 'tipo_aero_partida', 'nome_aeroporto_partida', 'continente_partida', 'pais_partida', 'cidade_partida')
            dim_partida = dim_partida.dropDuplicates(['aeroporto_partida'])

            # DIMENSAO DESTINO
            dim_destino = df_enriched_reordered.select('destino_id','aeroporto_chegada', 'tipo_aero_chegada', 'nome_aeroporto_chegada', 'continente_chegada', 'pais_chegada', 'cidade_chegada')
            dim_destino = dim_destino.dropDuplicates(['aeroporto_chegada'])

            # DIMENSAO SERVICO
            dim_servico = df_enriched_reordered.select('servico_id','cod_tipo_servico', 'aplicacao_servico', 'tipo_servico_operacao', 'tipo_servico_desc')
            dim_servico = dim_servico.dropDuplicates(['cod_tipo_servico', 'aplicacao_servico', 'tipo_servico_operacao', 'tipo_servico_desc'])

            # DIMENSAO AERONAVE
            dim_aeronave = df_enriched_reordered.select('aeronave_id','matricula_aeronave', 'aeronave_modelo_icao', 'aeronave_operador')
            dim_aeronave = dim_aeronave.dropDuplicates(['matricula_aeronave', 'aeronave_modelo_icao', 'aeronave_operador'])


            # TABELA FATO
            fato_voo = df_enriched_reordered.alias('f') \
                .join(broadcast(dim_tempo).alias('t'),
                    (F.col('f.data_manobra') == F.col('t.data_manobra')) & (F.col('f.ano') == F.col('t.ano')) & (F.col('f.mes') == F.col('t.mes')) & (F.col('f.dia_semana') == F.col('t.dia_semana')),
                    'left') \
                .join(broadcast(dim_partida).alias('p'), F.col('f.aeroporto_partida') == F.col('p.aeroporto_partida'), 'left') \
                .join(broadcast(dim_destino).alias('d'), F.col('f.aeroporto_chegada') == F.col('d.aeroporto_chegada'), 'left') \
                .join(broadcast(dim_servico).alias('s'), F.col('f.cod_tipo_servico') == F.col('s.cod_tipo_servico'), 'left') \
                .join(broadcast(dim_aeronave).alias('a'),
                    (F.col('f.matricula_aeronave') == F.col('a.matricula_aeronave')) &
                    (F.col('f.aeronave_modelo_icao') == F.col('a.aeronave_modelo_icao')) &
                    (F.col('f.aeronave_operador') == F.col('a.aeronave_operador')),
                    'left') \
                .select(
                    'f.voo_id',
                    'f.numero_voo',
                    'f.natureza_operacao',
                    'f.qtd_pax_local',
                    'f.qtd_pax_conexao_domestico',
                    'f.qtd_pax_conexao_internacional',
                    'f.qtd_correio',
                    'f.qtd_carga',
                    'f.pandemia_decreto',
                    'f.atraso',
                    't.tempo_id',
                    'p.partida_id',
                    'd.destino_id',
                    's.servico_id',
                    'a.aeronave_id'
                )

            output_path = f's3a://{bucket}/gold'
            dim_tempo.write.mode('overwrite').parquet(f'{output_path}/anac_movimentacoes/dim_tempo')
            dim_partida.write.mode('overwrite').parquet(f'{output_path}/anac_movimentacoes/dim_partida')
            dim_destino.write.mode('overwrite').parquet(f'{output_path}/anac_movimentacoes/dim_destino')
            dim_servico.write.mode('overwrite').parquet(f'{output_path}/anac_movimentacoes/dim_servico')
            dim_aeronave.write.mode('overwrite').parquet(f'{output_path}/anac_movimentacoes/dim_aeronave')
            fato_voo.write.mode('overwrite').parquet(f'{output_path}/anac_movimentacoes/fato_voo')

        except Exception as e:
            logging.error(f'Icao Airports Files Processing failed: {e}')
        finally:
            spark.stop()


    scraping = scraping_and_save_to_s3()
    transform_tasks = [
        transform_anac_mov_files(),
        transform_iata_service_file(),
        transform_airports_file()
    ]
    enrich_task = enrich_anac_mov_files()

    scraping >> transform_tasks
    transform_tasks >> enrich_task

anac_etl()
