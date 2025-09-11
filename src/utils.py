from pyspark.sql import SparkSession
from airflow.models import Variable

def create_spark(app_name: str = "ANAC_Data_Processing"):
    aws_access_key = Variable.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = Variable.get('AWS_SECRET_ACCESS_KEY')

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config('spark.hadoop.fs.s3a.access.key', aws_access_key)
        .config('spark.hadoop.fs.s3a.secret.key', aws_secret_key)
        .config('spark.hadoop.fs.s3a.endpoint', 's3.amazonaws.com')
        .config('spark.local.dir', '/tmp/spark')
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'true')
        .config('spark.driver.memory', '6g')
        .config('spark.executor.memory', '8g')
        .config('spark.executor.memoryOverhead', '1g')
        .config('spark.sql.shuffle.partitions', '16')
        .config('spark.ui.enabled', 'false')
        .getOrCreate()
    )
    return spark
