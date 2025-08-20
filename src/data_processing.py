from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, when, to_timestamp, concat_ws, lit, to_date, date_format

class DataProcessor:
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
    
    def read_file(self, schema, path, sep): 
        df = self.spark.read.option("header", "true").option("nullValue" ,"null").schema(schema).csv(path, sep=sep)
        return df

    def create_schema(self, fields: list):
        schema_fields = [
            StructField(name, type, nullable) for name, type, nullable in fields
        ]
        return StructType(schema_fields)
    
    def select_and_rename_columns(self, df, column_dict: dict):
        cols = [col(old_name).alias(new_name) for old_name, new_name in column_dict.items()]
        return df.select(*cols)
    
if __name__ == "__main__": 
    spark_session = SparkSession.builder \
        .appName("ANAC_Data_Processing") \
        .getOrCreate()
    
    processor = DataProcessor(spark_session) 

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
    df = processor.read_file(schema=schema,path='./data/raw/anac_csv/Movimentacoes_Aeroportuarias_201901.csv', sep=';')
    
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
    df_filtered = processor.select_and_rename_columns(df,anac_mov_columns)

    df_filtered.show()
    spark_session.stop()