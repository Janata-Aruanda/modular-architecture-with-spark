from extract import extract_csv, save_csv
from transformar import configure_session, load_data
import pyspark.sql.functions as f

def main(url: str, path_output: str, input_data: str):

    # Extraí o conteúdo CSV
    csv_content = extract_csv(url)

    # Salva o arquivo CSV no caminho especificado
    save_csv(csv_content, path_output) 

    # Sessão Spark
    spark = configure_session()

    # Carregando os dados 
    df = load_data(spark, input_data)

    # Exibindo o Schema
    df.printSchema()

    # Realizando análise dos dados com agregação 
    (
        df
        .groupBy('Sex')
        .agg(
            f.mean("Age").alias("med_idade"),
            f.min("Age").alias("idade_minima"),
            f.max("Age").alias("idade_max"),
            f.stddev("Age").alias("desvio_padrao_idade")
        )
        .show()
    )

    # Parando a sessão Spark
    spark.stop()

if __name__ == '__main__':  # Corrigido: '__main__' ao invés de 'main'

    url = 'https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv'
    path_output = "../pypspark_etl/data/titanic.csv"

    # Caminho do arquivo
    input_data = './data/titanic.csv'

    main(url, path_output, input_data)
