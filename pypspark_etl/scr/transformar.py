from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Criando a sessão no Spark
def configure_session() -> SparkSession:
    spark = SparkSession.builder.appName("SparkETL").getOrCreate()
    return spark

# Lendo os dados
def load_data(spark, input_data: str):
    df_titanic = (
        spark
        .read
        .format('csv')
        .option('delimiter', ';')
        .option('header', True)
        .option('inferSchema', True)  # Corrigido: 'inferShema' -> 'inferSchema'
        .load(input_data)  # input_data = './data/titanic.csv'
    )
    return df_titanic
