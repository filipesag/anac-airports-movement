import logging
from include.transform.data_enriching import DataEnriching
from include.transform.data_processing import DataProcessor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
import traceback
import sys
import os

sys.path.append('/usr/local/airflow/include')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

def enrich_anac_mov_files():
    spark = None
    try:
        logger.info('Starting Anac Files Enriching')

        spark = SparkSession.builder \
            .appName('ANAC_Data_Processing') \
            .config('spark.master', 'local[2]') \
            .config('spark.sql.adaptive.enabled', 'true') \
            .getOrCreate()
        
        # Configurar S3
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoop_conf.set('fs.s3a.access.key', os.environ.get('AWS_ACCESS_KEY_ID', ''))
        hadoop_conf.set('fs.s3a.secret.key', os.environ.get('AWS_SECRET_ACCESS_KEY', ''))
        hadoop_conf.set('fs.s3a.endpoint', 's3.sa-east-1.amazonaws.com')
        hadoop_conf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        hadoop_conf.set('fs.s3a.connection.ssl.enabled', 'true')
        spark.conf.set('spark.sql.legacy.parquet.int96RebaseModeInWrite', 'LEGACY')

        logger.info('✅ SparkSession set')

        bucket = 'anac-mov'
        processor = DataProcessor(spark)
        enrich = DataEnriching(spark)

        
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
        df_enriched = enrich.add_total_pax(df_enriched)

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
        
        selected_columns = ['numero_voo','qtd_pax_local','qtd_pax_conexao_domestico','qtd_pax_conexao_internacional','total_pax', 
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
        df_enriched_reordered = processor.replace_null_values(df_enriched_reordered, ['numero_voo','qtd_pax_local','qtd_pax_conexao_domestico','total_pax','qtd_pax_conexao_internacional', 
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
                'f.total_pax',
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

        logger.info('Files has been enriched and join performed - Save in Gold Layer')

    except Exception as e:
        logger.error(f'Icao Airports Files Processing failed: {e}',exc_info=True)
        logger.error(traceback.format_exc())
        raise
    finally:
        if spark is not None:
            spark.stop()
            logger.info('SparkSession finished')

def main():
    enrich_anac_mov_files()

if __name__ == '__main__':
    main()