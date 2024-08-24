from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Criando a sessão no Spark
def configure_session() -> SparkSession:
    spark = SparkSession.builder.appName("SparkETL").getOrCreate()
    return spark

# Lendo os dados
def load_data(spark, input_data: str):
    df_enem_2020 = (
        spark
        .read
        .format('csv')
        .option('delimiter', ';')
        .option('header', True)
        .option('inferSchema', True)
        .load(input_data)
    )
    return df_enem_2020

def transformacoes(spark, df_enem_2020):
    df_enem_2020.createOrReplaceTempView('enem_2020')

    notas_matematicas = spark.sql(
        '''
        SELECT COUNT(*) AS total_pessoas
        FROM enem_2020
        WHERE SG_UF_ESC = 'RJ'
        AND TP_SEXO = 'M'
        AND NU_NOTA_MT > 600
        '''
    )

    media_matematica = spark.sql(
        '''
        SELECT NO_MUNICIPIO_ESC, AVG(NU_NOTA_MT) AS media_nota_mt
        FROM enem_2020
        GROUP BY NO_MUNICIPIO_ESC
        ORDER BY media_nota_mt DESC
        LIMIT 1
        '''
    )

    qt_recife = spark.sql(
        '''
        SELECT COUNT(*) AS total_pessoas
        FROM enem_2020
        WHERE NO_MUNICIPIO_ESC = 'Recife'
        AND NO_MUNICIPIO_PROVA = 'Recife'
        '''
    )

    media_matematica_fem = spark.sql(
        '''
        SELECT AVG(NU_NOTA_MT) AS media_nota_mt
        FROM enem_2020
        WHERE NU_ANO = 2020
        AND TP_SEXO = 'F'
        '''
    )

    cidade_maior_nota_cn = spark.sql(
        '''
        SELECT NO_MUNICIPIO_ESC, MAX(NU_NOTA_CN) AS maior_nota_cn
        FROM enem_2020
        GROUP BY NO_MUNICIPIO_ESC
        ORDER BY maior_nota_cn DESC
        LIMIT 1
        '''
    )

    return {
        "notas_matematicas": notas_matematicas,
        "media_matematica": media_matematica,
        "qt_recife": qt_recife,
        "media_matematica_fem": media_matematica_fem,
        "cidade_maior_nota_cn": cidade_maior_nota_cn
    }
