# Databricks notebook source
# Define os parâmetros da Job
dbutils.widgets.text('p_path_abfss','')
dbutils.widgets.text('p_layer','')
dbutils.widgets.text('p_file_name_raw','')
dbutils.widgets.text('p_file_type_raw','')
dbutils.widgets.text('p_mode_write_bronze','')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Premissa: Escolher um conjunto de dados do Kaggle relacionado a vendas. 
# MAGIC Obs.: Certifique-se de que o conjunto de dados inclui informações como datas, produtos, quantidades vendidas, etc.
# MAGIC
# MAGIC **Dataset escolhido**: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data

# COMMAND ----------

# DBTITLE 1,Importa bilbiotecas
# Importando Bibliotecas
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ####Funções para leitura, processamento e gravação de dados

# COMMAND ----------

# DBTITLE 1,Cria função para leitura de arquivo .csv
def ler_csv(path_abfss :str, layer :str, file_name_raw :str, file_type_raw :str) -> DataFrame:
    """
    Lê um arquivo CSV do ADSL Gen2.

    Args:
        layer (str): Camada do arquivo.
        file_name (str): Nome do arquivo.
        file_type (str): Tipo do arquivo.

    Returns:
        DataFrame: O DataFrame resultante da leitura do arquivo CSV.
    """
    try:
        # Opção de leitura do arquivo CSV
        infer_schema = "true"
        first_row_is_header = "true"
        delimiter = ","

        # Lê arquivo csv
        df = spark.read.format(file_type_raw)\
            .option('header',first_row_is_header)\
            .option('inferschema',infer_schema)\
            .load(f'{path_abfss}/{layer}/{file_name_raw}.{file_type_raw}')

        return df
    except Exception as e:
        print(f"Erro ao ler o arquivo CSV: {e}")
        return None

# COMMAND ----------

# DBTITLE 1,Cria função para inserir coluna row_ingestion_timestamp
def add_timestamp(df:DataFrame) -> DataFrame:
    """
    Adiciona uma coluna com a data e hora atuais a um DataFrame do Spark.

    Args:
        df (DataFrame): DataFrame do Spark.
        nome_coluna (str): Nome da coluna a ser adicionada.

    Returns:
        DataFrame: O DataFrame original com a nova coluna adicionada.
    """
    try:
        # Adiciona uma coluna com a data e hora atuais
        df_timestamp = df.withColumn('row_ingestion_timestamp', current_timestamp())
        
        return df_timestamp
    except Exception as e:
        print(f"Erro ao adicionar timestamp: {e}")
        return None

# COMMAND ----------

def gravar_parquet(df :DataFrame, destination_path :str, mode_write_bronze :str) -> None:
    """
    Grava os dados de um DataFrame no formato Parquet.

    Args:
        df (DataFrame): DataFrame do Spark.
        caminho_destino (str): Caminho de destino para salvar o arquivo Parquet.
    """
    # Grava o DataFrame no formato Parquet
    df.write.mode(mode_write_bronze).parquet(destination_path)

    print("Dados salvos com sucesso no formato Parquet.")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Premissa: Carregue o conjunto de dados no Databricks.
# MAGIC Explore o esquema dos dados e faça ajustes conforme necessário.

# COMMAND ----------

# DBTITLE 1,Lê arquivo .csv e adiciona coluna de controle row_ingestion_timestamp
# Parâmetros de leitura de arquivo
path_abfss = dbutils.widgets.get('p_path_abfss') #"abfss://datum-metastore@proofconceptdatum.dfs.core.windows.net"
layer = dbutils.widgets.get('p_layer') #"raw/olistbr-brazilian-ecommerce"
file_name_raw = dbutils.widgets.get('p_file_name_raw') #"olist_customers_dataset"
file_type_raw = dbutils.widgets.get('p_file_type_raw') #"csv"

# Lê o CSV
df = ler_csv(path_abfss=path_abfss, layer=layer, file_name_raw=file_name_raw, file_type_raw=file_type_raw)

# Adiciona coluna row_ingestion_timestamp
df_rits = add_timestamp(df=df)

display(df_rits)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Premissa: Grave os dados transformados e agregados em um formato Parquet para persistência eficiente.

# COMMAND ----------

# Parâmetros de gravação de arquivo
file_name_bronze = file_name_raw
destination_path = f'{path_abfss}/bronze/olistbr-brazilian-ecommerce/{file_name_bronze}'
mode_write_bronze = dbutils.widgets.get('p_mode_write_bronze') #'overwrite'

# Salva arquivo no ADLS (Azure Data Lake Storage) em formato parquet, na camada bronze
gravar_parquet(df_rits, destination_path=destination_path, mode_write_bronze=mode_write_bronze)
