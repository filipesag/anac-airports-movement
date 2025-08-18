from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, when, to_timestamp, concat_ws, lit, to_date, date_format

class DataProcessor:
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
    
    def set_schema(self):
        return StructType([
            StructField("ANO", StringType(), True),
            StructField("MES", StringType(), True),
            StructField("NR_AEROPORTO_REFERENCIA", StringType(), True),
            StructField("NR_MOVIMENTO_TIPO", StringType(), True),
            StructField("NR_AERONAVE_MARCAS", StringType(), True),
            StructField("NR_AERONAVE_TIPO", StringType(), True),
            StructField("NR_AERONAVE_OPERADOR", StringType(), True),
            StructField("NR_VOO_OUTRO_AEROPORTO", StringType(), True),
            StructField("NR_VOO_NUMERO", StringType(), True),
            StructField("NR_SERVICE_TYPE", StringType(), True),
            StructField("NR_NATUREZA", StringType(), True),
            StructField("DT_PREVISTO", TimestampType(), True),
            StructField("HH_PREVISTO", TimestampType(), True),
            StructField("DT_CALCO", TimestampType(), True),
            StructField("HH_CALCO", TimestampType(), True),
            StructField("DT_TOQUE", TimestampType(), True),
            StructField("HH_TOQUE", TimestampType(), True),
            StructField("NR_CABECEIRA", StringType(), True),
            StructField("NR_BOX", StringType(), True),
            StructField("NR_PONTE_CONECTOR_REMOTO", StringType(), True),
            StructField("NR_TERMINAL", StringType(), True),
            StructField("QT_PAX_LOCAL", IntegerType(), True),
            StructField("QT_PAX_CONEXAO_DOMESTICO", IntegerType(), True),
            StructField("QT_PAX_CONEXAO_INTERNACIONAL", IntegerType(), True),
            StructField("QT_CORREIO", DoubleType(), True),
            StructField("QT_CARGA", DoubleType(), True),
        ]) 

    def read_file(self, schema, path, sep): 
        df = self.spark.read.option("header", "true").option("nullValue" ,"null").schema(schema).csv(path, sep=sep)
        return df
    
    def select_columns(self, df):
        new_df = df.select(col("ANO").alias("ano"),
                           col("MES").alias("mes"),
                           col("NR_AEROPORTO_REFERENCIA").alias("aeroporto_ref"),
                           col("NR_MOVIMENTO_TIPO").alias("tipo_movimento"),
                           col("NR_AERONAVE_MARCAS").alias("matricula_aeronave"),
                           col("NR_AERONAVE_TIPO").alias("aeronave_modelo_icao"),
                           col("NR_AERONAVE_OPERADOR").alias("aeronave_operador"),
                           col("NR_VOO_OUTRO_AEROPORTO").alias("aeroporto_outro"),
                           col("NR_VOO_NUMERO").alias("numero_voo"),
                           col("NR_SERVICE_TYPE").alias("tipo_servico"),
                           col("NR_NATUREZA").alias("natureza_operacao"),
                           col("DT_PREVISTO").alias("data_prevista_movimento"),
                           col("HH_PREVISTO").alias("hora_prevista_movimento"),
                           col("DT_CALCO").alias("data_calco"),
                           col("HH_CALCO").alias("hora_calco"),
                           col("DT_TOQUE").alias("data_manobra"),
                           col("HH_TOQUE").alias("hora_manobra"),
                           col("QT_PAX_LOCAL").alias("qtd_pax_local"),
                           col("QT_PAX_CONEXAO_DOMESTICO").alias("qtd_pax_conexao_domestico"),
                           col("QT_PAX_CONEXAO_INTERNACIONAL").alias("qtd_pax_conexao_internacional"),
                           col("QT_CORREIO").alias("qtd_correio"),
                           col("QT_CARGA").alias("qtd_carga")
                           )
        return new_df

if __name__ == "__main__": 
    spark_session = SparkSession.builder \
        .appName("ANAC_Data_Processing") \
        .getOrCreate()
    
    processor = DataProcessor(spark_session) 
    schema = processor.set_schema()
    df = processor.read_file(schema=schema,path='./data/Movimentacoes_Aeroportuarias_201901.csv', sep=';')

    df_filtered = processor.select_columns(df)

    df_filtered.show()
    spark_session.stop()