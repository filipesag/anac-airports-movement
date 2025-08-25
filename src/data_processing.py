from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, when, to_timestamp, concat_ws, lit, to_date, date_format

class DataProcessor:
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
    
    def read_file(self, schema, path, sep): 
        df = self.spark.read.option("header", "true").option("nullValue" ,"null").option("enforceSchema", "false").schema(schema).csv(path, sep=sep)
        return df

    def create_schema(self, fields: list):
        schema_fields = [
            StructField(name, type, nullable) for name, type, nullable in fields
        ]
        return StructType(schema_fields)
    
    def select_and_rename_columns(self, df, column_dict: dict):
        cols = [col(old_name).alias(new_name) for old_name, new_name in column_dict.items()]
        return df.select(*cols)
    
    def replace_null_values(self, df, columns:list, value):
        return df.na.fill(value, subset=columns)

    def replace_column_values(self, df, values:dict, columns:list):
        return df.replace(values, subset=columns)
    

if __name__ == "__main__":

    spark = SparkSession.builder.appName("ANAC").getOrCreate()
    print(spark.version)
    
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

    df = processor.read_file(schema=schema, path='./data/raw/anac_csv/2019/Movimentacoes_Aeroportuarias_202207.csv', sep=';')
    df = processor.select_and_rename_columns(df,anac_mov_columns)

    df = processor.replace_null_values(df, ['qtd_correio',
                                                'qtd_carga',
                                                'qtd_pax_conexao_internacional',
                                                'qtd_pax_conexao_domestico',
                                                'qtd_pax_local'], -1)
    
    df = processor.replace_null_values(df, ['ano'], 'Ano não informado')
    df = processor.replace_null_values(df, ['mes'], 'nops')
    df = processor.replace_null_values(df, ['aeroporto_ref','aeroporto_outro'], 'Aeroporto não informado')
    df = processor.replace_null_values(df, ['matricula_aeronave'], 'Matrícula não informada')
    df = processor.replace_null_values(df, ['aeronave_modelo_icao'], 'Modelo não informado')
    df = processor.replace_null_values(df, ['aeronave_operador'], 'Operador não informado')
    df = processor.replace_null_values(df, ['numero_voo'], 'Número de voo não informado')
    df = processor.replace_null_values(df, ['tipo_servico'], 'Serviço não informado')
    df = processor.replace_null_values(df, ['natureza_operacao'], 'Tipo de operação não informada')
    df = processor.replace_null_values(df, ['data_prevista_movimento','data_calco','data_manobra'], 'Data não informada')
    df = processor.replace_null_values(df, ['hora_prevista_movimento','hora_calco','hora_manobra'], 'Hora não informada')

    df = processor.replace_column_values(df, {"1":"Janeiro"}, ['mes'])
   

    filtered_df = df.where(col("qtd_carga") == -1)
    print(filtered_df.show())
