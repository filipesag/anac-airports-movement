from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.column import Column

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
    
    def replace_null_values(self, df: DataFrame, columns: list, value_replace):
        for col_name in columns:
            replacement = value_replace if isinstance(value_replace, Column) else F.lit(value_replace)
            df = df.withColumn(
                col_name,
                F.when(
                    (F.col(col_name).isNull()) | (F.col(col_name) == "") | (F.col(col_name) == "\\N"),
                    replacement
                ).otherwise(F.col(col_name))
            )
        return df

    def replace_column_values(self, df, values:dict, columns:list):
        return df.replace(values, subset=columns)

    def clean_string_columns(self, df: DataFrame) -> DataFrame:
        string_cols = [c.name for c in df.schema.fields if c.dataType.simpleString() == "string"]
        
        for col_name in string_cols:
            df = df.withColumn(col_name, F.regexp_replace(F.col(col_name), u'\u200b', ''))
            df = df.withColumn(col_name, F.trim(F.col(col_name)))
        
        return df
    

if __name__ == "__main__":

    spark = SparkSession.builder.appName("ANAC").getOrCreate()
    print(spark.version)
    
    sc = spark.sparkContext
    print("Driver:", sc.master)
    print("Cores disponíveis:", sc.defaultParallelism)
    print("Executors ativos:", sc._jsc.sc().getExecutorMemoryStatus().keys())

