# Databricks notebook source
# DBTITLE 1,Lê arquivo
# MAGIC %md
# MAGIC ### Lê arquivo kaggle.json com as credenciais de acesso à API e cria variavies de conexão

# COMMAND ----------

# DBTITLE 1,Importa bibliotecas
# Importa bibliotecas
import os
from kaggle.api.kaggle_api_extended import KaggleApi    # Lib já instalada no cluster

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
def read_kaggle_credentials(path :str, path_layer_name :str, file_name :str) -> tuple:
    """
    Lê as credenciais do Kaggle de um arquivo JSON armazenado no caminho especificado.

    Parâmetros:
    path (str): Caminho base até o diretório.
    path_layer_name (str): Nome da subpasta no caminho base.
    file_name (str): Nome do arquivo JSON.

    Retorna:
    tuple: Contém o nome de usuário e a chave do Kaggle, e um booleano indicando se ambas as credenciais são não nulas.
    """

    # Cria o caminho completo
    json_file_path = f'{path}/{path_layer_name}/{file_name}'

    # Lê arquivo .json
    json_df = spark.read.format('json').option('header','true').option('inferschema','true').load(json_file_path)

    # Pega as credenciais de conexão
    KAGGLE_USERNAME = json_df.select(json_df.username).take(1)[0]['username']
    KAGGLE_KEY = json_df.select(json_df.key).take(1)[0]['key']

    return KAGGLE_USERNAME, KAGGLE_KEY

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

def download_kaggle_dataset(dataset_name: str, path_dataset_save: str, KAGGLE_USERNAME: str, KAGGLE_KEY: str) -> None:
    """
    Baixa e extrai um dataset do Kaggle.

    Parâmetros:
    dataset_name (str): Nome do dataset no Kaggle.
    path_dataset_save (str): Caminho local onde o dataset será salvo.
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

    # Verifica se pasta destino se não existir
    os.makedirs(path_dataset_save, exist_ok=True)

    # Tentativa de download e extração do dataset
    try:
        api.dataset_download_files(dataset_name, path=path_dataset_save, unzip=True )
    except Exception as e:
        print(f"Erro ao baixar o dataset: {e}")
        print("Verifique o nome do dataset e sua conexão de internet")

# COMMAND ----------

# Informa os parâmetros de pasta
path = 'abfss://datum-metastore@proofconceptdatum.dfs.core.windows.net'
path_layer_name = 'raw'
file_name = 'kaggle.json'

dataset_name = 'marian447/retail-store-sales-transactions'
path_dataset_save = 'Assets/dataset_kaggle/'

# COMMAND ----------



credentials = read_kaggle_credentials(path=path,path_layer_name=path_layer_name,file_name=file_name)
user = credentials[0]
key = credentials[1]

# Faz autentificação
autentifica_kaggle(KAGGLE_USERNAME=user,KAGGLE_KEY=key)

# Faz o download, extrai o dataset e salva na pasta especificada
download_kaggle_dataset(dataset_name=dataset_name, path_dataset_save=path_dataset_save, KAGGLE_USERNAME=user, KAGGLE_KEY=key)


# COMMAND ----------

df = spark.read.format('csv').load('/Workspace/Repos/edvaldo.gutierres@datacraft.com.br/poc_datum_databricks/Ingestion/Assets/dataset_kaggle/scanner_data.csv')
display(df)

# COMMAND ----------

file_path = '/Workspace/Repos/edvaldo.gutierres@datacraft.com.br/poc_datum_databricks/Ingestion/Assets/dataset_kaggle/scanner_data.csv'

if dbutils.fs.ls(file_path):
    df = spark.read.format('csv').load(file_path)
    display(df)
else:
    print(f"File not found: {file_path}")
