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
    
