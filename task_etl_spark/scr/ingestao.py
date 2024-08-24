import os
import subprocess
import zipfile
import logging
from tqdm import tqdm

def download_with_curl(url: str, output_dir: str):
    """Baixa o arquivo usando curl e mostra o progresso com tqdm."""
    zip_path = os.path.join(output_dir, "micro_dados_enem_2020.zip")
    
    # Certifica-se de que o diretório de saída existe
    os.makedirs(output_dir, exist_ok=True)
    
    # Comando curl para download com opção de progresso
    command = ["curl", "-L", "-o", zip_path, url]
    
    # Executa o comando curl e exibe o progresso
    try:
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as proc:
            for line in proc.stderr:
                if "%" in line:
                    progress = line.split()[1]
                    # Atualiza a barra de progresso
                    tqdm.write(progress)
                    
        logging.info(f"Download concluído com sucesso. Arquivo salvo em: {zip_path}")
        
    except subprocess.CalledProcessError as e:
        logging.error(f"Erro durante o download com curl: {e.stderr}")
        raise

def extract_data(url: str, output_dir: str, name_file: str):
    """Baixa e extrai o arquivo ZIP especificado."""
    # Baixa o arquivo ZIP
    download_with_curl(url, output_dir)
    
    zip_path = os.path.join(output_dir, "micro_dados_enem_2020.zip")
    
    # Extrai o arquivo desejado
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Verifica se o arquivo desejado está no ZIP
            if name_file in zip_ref.namelist():
                total_files = len(zip_ref.namelist())
                with tqdm(total=total_files, desc='Extraindo arquivos', unit='file') as pbar:
                    for file in zip_ref.namelist():
                        if file == name_file:
                            zip_ref.extract(file, output_dir)
                        pbar.update(1)
                logging.info(f"Arquivo extraído com sucesso: {name_file}")
            else:
                logging.error(f"Arquivo {name_file} não encontrado no ZIP.")
                raise FileNotFoundError(f"Arquivo {name_file} não encontrado no ZIP.")
    
    except Exception as e:
        logging.error(f"Erro durante a extração do arquivo: {e}")
        raise
    
    # Remove o arquivo ZIP após a extração
    os.remove(zip_path)

if __name__ == '__main__':
    logging.basicConfig(filename='extract.log', level=logging.INFO)
    url = "https://download.inep.gov.br/microdados/microdados_enem_2020.zip"
    
    # Ajusta o diretório de saída para ../data
    output_dir = '../data'
    name_file = 'DADOS/MICRODADOS_ENEM_2020.csv'

    extract_data(url, output_dir, name_file)
