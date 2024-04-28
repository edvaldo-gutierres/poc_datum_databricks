# Databricks notebook source
# Define os parâmetros da Job
dbutils.widgets.text('p_path','')
dbutils.widgets.text('p_dataset_name','')
dbutils.widgets.text('p_path_layer_name','')

# COMMAND ----------

# DBTITLE 1,Lê arquivo
# MAGIC %md
# MAGIC ### Lê arquivo kaggle.json com as credenciais de acesso à API e cria variavies de conexão

# COMMAND ----------

# DBTITLE 1,Copia arquivo kaggle.json para ambiente local
dbutils.fs.cp("abfss://datum-metastore@proofconceptdatum.dfs.core.windows.net/raw/kaggle.json", "file:/root/.kaggle/kaggle.json")

# COMMAND ----------

# DBTITLE 1,Importa bibliotecas
# Importa bibliotecas
from kaggle.api.kaggle_api_extended import KaggleApi    # Lib já instalada no cluster
from shutil import rmtree
import os
import json

# COMMAND ----------

# DBTITLE 1,Lista container configrados do ADLS Gen2
# Obs.: O cluster foi configurado para ler os dados do ADSL Gen2.

# Lista arquivos na pasta do container do ADLS Gen2 configurado
files = dbutils.fs.ls("abfss://datum-metastore@proofconceptdatum.dfs.core.windows.net/raw")
print(files)

# for file in files:
#     print(f"Name: {file.name}, Size: {file.size}, Is Directory: {file.isDir}")

# COMMAND ----------

# DBTITLE 1,Busca credenciais de Conexão
def read_kaggle_credentials(file_path_json: str) -> tuple:
    """
    Lê as credenciais do Kaggle de um arquivo JSON armazenado no caminho especificado.

    Parâmetros:
    file_path (str): Caminho completo até o arquivo JSON.

    Retorna:
    tuple: Contém o nome de usuário e a chave do Kaggle, e um booleano indicando se ambas as credenciais são não nulas.
    """

    try:
        # Lê o arquivo JSON
        with open(file_path_json, 'r') as file:
            credentials = json.load(file)
        
        # Extrai as credenciais
        KAGGLE_USERNAME = credentials['username']
        KAGGLE_KEY = credentials['key']
        
        # Verifica se ambas as credenciais são não nulas
        credentials_found = bool(KAGGLE_USERNAME and KAGGLE_KEY)
        return (KAGGLE_USERNAME, KAGGLE_KEY, credentials_found)
    except Exception as e:
        print(f"Erro ao ler as credenciais: {e}")
        return (None, None, False)

# COMMAND ----------

# DBTITLE 1,Cria função para verificar conexão com a API
def autentifica_kaggle(KAGGLE_USERNAME :str, KAGGLE_KEY :str) -> KaggleApi:
    """
    Autentica na API do Kaggle utilizando as credenciais fornecidas.

    Parâmetros:
    KAGGLE_USERNAME (str): Nome de usuário do Kaggle.
    KAGGLE_KEY (str): Chave de API do Kaggle.

    Retorna:
    KaggleApi: Instância da API do Kaggle autenticada.
    """

    # Define as variavéis de ambiente
    os.environ['KAGGLE_USERNAME'] = KAGGLE_USERNAME
    os.environ['KAGGLE_KEY'] = KAGGLE_KEY

    # Cria e autentica uma instância da API do Kaggle
    api = KaggleApi()
    api.authenticate()

    print('Conexão estabelecida com sucesso!')

    return api

# COMMAND ----------

# DBTITLE 1,Cria função que baixa e extrai um dataset do Kaggle e salva no Azure Data Lake Storage
def download_kaggle_dataset(dataset_name: str, path_adsl: str, KAGGLE_USERNAME: str, KAGGLE_KEY: str) -> None:
    """
    Baixa e extrai um dataset do Kaggle e salva no Azure Data Lake Storage.

    Parâmetros:
    dataset_name (str): Nome do dataset no Kaggle.
    path_adsl (str): Caminho no Azure Data Lake Storage onde o dataset será salvo.
    KAGGLE_USERNAME (str): Nome de usuário do Kaggle.
    KAGGLE_KEY (str): Chave de API do Kaggle.
    """
    
    # Tentativa de Autentificação
    try:
        api = autentifica_kaggle(KAGGLE_USERNAME, KAGGLE_KEY)
    except Exception as e :
        print(f"Erro de autenticação: {e}")
        print('Verifique se as credenciais estão corretas e se a biblioteca kaggle está instalada')
        return  # Encerra a função se a autenticação falhar

    # Pasta temporária no sistema de arquivos local para o download do dataset
    local_path = "/dbfs/tmp/kaggle_dataset"
    os.makedirs(local_path, exist_ok=True)

    # Tentativa de download e extração do dataset
    try:
        api.dataset_download_files(dataset_name, path=local_path, unzip=True )
    except Exception as e:
        print(f"Erro ao baixar o dataset: {e}")
        print("Verifique o nome do dataset e sua conexão de internet")
        return
    
    # Move o arquivo baixado para o ADLS
    try:
        for item in os.listdir(local_path):
            file_path = os.path.join(local_path, item)
            dbutils.fs.mv(f"file:{file_path}", os.path.join(path_adsl, item), recurse=True)
        print(f"Dataset {dataset_name} foi baixado e movido para {path_adsl} com sucesso.")
    except Exception as e:
        print(f"Erro ao mover o dataset para o ADLS: {e}")
        print("Verifique se o caminho do ADLS está correto e se você tem as permissões adequadas")
    finally:
        # Limpa a pasta temporária após o uso
        rmtree(local_path)



# COMMAND ----------

# Função principal que orquestra a execução das funções acima
def main():
    # Define os valores para os parâmetros (Valores passados na Job)

    # path = 'abfss://datum-metastore@proofconceptdatum.dfs.core.windows.net'
    # path_layer_name = 'raw'
    # file_name_json = 'kaggle.json'
    # dataset_name = 'marian447/retail-store-sales-transactions'

    path = dbutils.widgets.get('p_path')
    path_layer_name = dbutils.widgets.get('p_path_layer_name')
    dataset_name = dbutils.widgets.get('p_dataset_name')

    path_adsl = f'{path}/{path_layer_name}/'
    file_path_json = '/root/.kaggle/kaggle.json'

    # Recupera as credencias executando a função read_kaggle_credentials
    credentials = read_kaggle_credentials(file_path_json=file_path_json)
    user = credentials[0]
    key = credentials[1]

    # Faz o download, extrai o dataset e salva na pasta especificada executando a função download_kaggle_dataset
    download_kaggle_dataset(dataset_name=dataset_name, path_adsl=path_adsl, KAGGLE_USERNAME=user, KAGGLE_KEY=key)


# COMMAND ----------

# Bloco que verifica se o script é o ponto de entrada principal
if __name__ == "__main__":
    main()
