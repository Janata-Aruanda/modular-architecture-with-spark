from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def configure_session(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def load_data(spark: SparkSession, input_data: str, arqshema: str):
    data = spark.read.csv(input_data, inferSchema=True, header=False, schema=arqshema, encoding="UTF-8")
    return data

def transform_year(data):
    trans_year = data.withColumn('ano', F.year('data'))
    return trans_year