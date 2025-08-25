from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, date_format
import pyspark.sql.functions as F

class DataEnriching:

    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def add_flag_covid(self, df):
        df = df.withColumn("pandemia_decreto", when(col("data_manobra").between("2020-03-20", "2022-04-22"), True).otherwise(False))
        return df
    
    def add_flag_delay(self, df):
        time_diff_seconds = F.unix_timestamp(F.col("hora_manobra")) - F.unix_timestamp(F.col("hora_prevista_movimento"))
 
        delay_condition = (time_diff_seconds / 60 >= 30) & (F.col("natureza_operacao") == "Doméstico") | (time_diff_seconds / 60 >= 60) & (F.col("natureza_operacao") == "Internacional")
       
        df = df.withColumn("atraso", F.when(delay_condition, True).otherwise(False))
        return df
    
    def add_day_column(self, df):
        days = {
            "Monday": "Segunda-feira",
            "Tuesday": "Terça-feira",
            "Wednesday": "Quarta-feira",
            "Thursday": "Quinta-feira",
            "Friday": "Sexta-feira",
            "Saturday": "Sábado",
            "Sunday": "Domingo"
        }

        df = df.withColumn("dia_semana", date_format(col("data_manobra"), "EEEE"))
        
        df = df.withColumn("dia_semana",
        F.when(F.col("dia_semana") == "Monday", days["Monday"])
        .when(F.col("dia_semana") == "Tuesday", days["Tuesday"])
        .when(F.col("dia_semana") == "Wednesday", days["Wednesday"])
        .when(F.col("dia_semana") == "Thursday", days["Thursday"])
        .when(F.col("dia_semana") == "Friday", days["Friday"])
        .when(F.col("dia_semana") == "Saturday", days["Saturday"])
        .when(F.col("dia_semana") == "Sunday", days["Sunday"])
        .otherwise("Dia não informado")
        )
        return df
    
    def set_airports(self, df):
        df = df.withColumn(
            "aeroporto_partida",
            F.when(F.col("tipo_movimento") == "D", F.col("aeroporto_ref"))
            .when(F.col("tipo_movimento") == "P", F.col("aeroporto_outro"))
        ).withColumn(
            "aeroporto_chegada",
            F.when(F.col("tipo_movimento") == "P", F.col("aeroporto_ref"))
            .when(F.col("tipo_movimento") == "D", F.col("aeroporto_outro"))
        )
        return df
    