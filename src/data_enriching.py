from pyspark.sql import SparkSession

class DataEnriching:

    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session