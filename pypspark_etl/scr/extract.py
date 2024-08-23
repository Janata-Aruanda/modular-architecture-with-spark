import requests
import os

def extract_csv(url: str):
    ex_csv = requests.get(url)
    if ex_csv.status_code == 200:
        print(f"Sucesso, requisição: {ex_csv.status_code}")
    else:
        print(f"Falha ao acessar o arquivo CSV, Status da requisição: {ex_csv.status_code}")
    return ex_csv.text

def save_csv(csv_content: str, path_output: str):
    os.makedirs(os.path.dirname(path_output), exist_ok=True)  # Garante que o diretório exista
    with open(path_output, 'w', encoding='utf-8') as file:
        file.write(csv_content)
    print(f"Arquivo salvo como {path_output}")


