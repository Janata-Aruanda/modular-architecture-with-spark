from load import configure_session, load_data, transform_year

def main():
    # Configura a sessão Spark
    app_name = 'spark_modular'
    spark = configure_session(app_name)

    # Defina o esquema (exemplo de esquema)
    arqshema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"

    # Carrega os dados
    input_data = "../dataset/despachantes.csv"
    df = load_data(spark, input_data, arqshema)
    df.show()

    # Criar um colunar apartir de um coluna existtente 
    trans_year = transform_year(df)
    # Mostra os dados carregados
    trans_year.show()

if __name__ == "__main__":
    main()


# spark-submit --master local[*] main.py