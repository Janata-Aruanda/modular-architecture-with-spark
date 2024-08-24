from transf import configure_session, load_data, transformacoes

def main(input_data: str):
    spark = configure_session()
    df = load_data(spark, input_data)
    
    # Executa as transformações e obtém os DataFrames resultantes
    resultados = transformacoes(spark, df)
    
    # Exibe os resultados
    for nome, resultado in resultados.items():
        print(f"Resultados para {nome}:")
        resultado.show() 
    
    spark.stop()

if __name__ == '__main__':
    input_data = '../data/DADOS/MICRODADOS_ENEM_2020.csv'
    main(input_data)
