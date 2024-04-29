# Databricks notebook source
# MAGIC %md
# MAGIC ### Premissas:
# MAGIC - Nome: dim_customers
# MAGIC - Descrição da tabela: Tabela composta por variáveis qualitativas de clientes.
# MAGIC - Tipo: SCD-1

# COMMAND ----------

# Importa bibliotecas
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

# Cria dataframe com dados para a criação da dimensão
df_dimensao = spark.sql("""
SELECT DISTINCT
  customer_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state
FROM poc_datum.olist.customers_silver 
-- WHERE customer_unique_id = '7e4bebe20140a71b34263a659ba1ce11'
""")

display(df_dimensao)

# COMMAND ----------

# Primeira carga

# Cria DataFrame com os dados de "NÃO INFORMADO"
data = [(-1, -1, "Não informado", "Não informado")]
column = df_dimensao.schema.fieldNames()
df = spark.createDataFrame( data, schema=column )
# df.show()

# Faz o join com o dataframe com dados para a criação da dimensão
dim_customers = df.union(df_dimensao)
# dim_customers.show()

# Inseri as colunas default de dimensão
dim_customers = dim_customers.withColumn( "row_ingestion_timestamp", current_timestamp() ) \
    .withColumn( "row_version", lit(1) ) \
    .withColumn( "row_current_indicator", lit(True) ) \
    .withColumn( "row_effective_date", to_timestamp( lit('1900-01-01 00:00:00'), "yyyy-MM-dd HH:mm:ss") ) \
    .withColumn( "row_expiration_date", to_timestamp( lit('2200-01-01 00:00:00') , "yyyy-MM-dd HH:mm:ss" ) )

# Inseri coluna SK
# dim_doenca = dim_doenca.withColumn( 'sk_dim_doenca', monotonically_increasing_id() ) 
dim_customers = dim_customers.withColumn( 
    'sk_dim_customers', 
    sha2(concat_ws("|", 
        dim_customers.row_ingestion_timestamp, 
        dim_customers.customer_id, 
        dim_customers.customer_zip_code_prefix), 256))

# Ordena as colunas
dim_customers_select = dim_customers.select( 
    'sk_dim_customers',
    'row_ingestion_timestamp',
    'row_version',
    'row_current_indicator',
    'row_effective_date',
    'row_expiration_date',
    'customer_id',
    'customer_zip_code_prefix',
    'customer_city',
    'customer_state'
    )

display(dim_customers_select)

# COMMAND ----------

table_name = 'dim_customers'
spark.sql('USE olist')
spark.sql(f'DROP TABLE IF EXISTS inter_sales')
dim_customers_select.write.format("delta").mode('overwrite').saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga Diferencial (Upsert)

# COMMAND ----------

#teste
data = [('2455a94ebab82b39829283f823a69bba',39900,'almenara1','MG')]
column = ['customer_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state']

df_dimensao = spark.createDataFrame(data, schema=column)
df_dimensao.show()

# COMMAND ----------

# Dados Novos
df_origem = df_dimensao

# Dados da dimensão
df_destino = spark.sql("""
    SELECT   
        customer_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state 
    FROM 
        poc_datum.olist.dim_customers 
    ORDER BY customer_id """)

# Realize o EXCEPT (retornar apenas registros novos)
df_dados_novos = df_origem.exceptAll(df_destino)

display(df_dados_novos)

# Cria uma tabela temporária
df_dados_novos.createOrReplaceTempView("temp_dados_novos")

# COMMAND ----------

# Paramêtros
table_merge = 'dim_customers'
change_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
print(change_date)

# COMMAND ----------

# spark.sql('USE olist')

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW dados_novos AS 
SELECT
     sha2(concat_ws("|",  '{change_date}',  a.customer_id, a.customer_zip_code_prefix), 256) AS sk_dim_customers
    ,a.customer_id
    ,a.customer_zip_code_prefix
    ,a.customer_city
    ,a.customer_state
    ,to_timestamp('{change_date}')  AS change_date
    ,(
        SELECT
            MAX(b.sk_dim_customers)
        FROM
            dim_customers as b
        WHERE
            a.customer_id = b.customer_id
    ) AS max_sk_dim_customers
    ,COALESCE(
        (
            SELECT
                MAX(c.row_version) + 1
            FROM
                dim_customers as c
            WHERE
                a.customer_id = c.customer_id
        ), 1
    ) AS max_row_version
FROM
    temp_dados_novos AS a
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM
# MAGIC     dados_novos AS a

# COMMAND ----------

spark.sql(f""" 
MERGE INTO {table_merge} as destino
USING dados_novos 
ON destino.sk_dim_customers = dados_novos.max_sk_dim_customers

WHEN MATCHED THEN 
  UPDATE SET
   destino.row_expiration_date = to_timestamp('{change_date}') --dados_novos.change_date
  ,destino.row_current_indicator = False
  """)

# COMMAND ----------

spark.sql(f"""
MERGE INTO {table_merge} as destino
USING dados_novos 
ON destino.customer_id = dados_novos.customer_id
AND destino.customer_city = dados_novos.customer_city

WHEN NOT MATCHED 
  THEN INSERT (
    sk_dim_customers
    , row_ingestion_timestamp
    ,row_version
    ,row_current_indicator
    ,row_effective_date
    ,row_expiration_date
    ,customer_id
    ,customer_zip_code_prefix
    ,customer_city
    ,customer_state
  )
  VALUES (
    dados_novos.sk_dim_customers
    ,to_timestamp('{change_date}') --dados_novos.change_date
    ,dados_novos.max_row_version
    ,1
    ,to_timestamp('{change_date}') --dados_novos.change_date
    ,to_timestamp( '2200-01-01 00:00:00')
    ,dados_novos.customer_id
    ,dados_novos.customer_zip_code_prefix
    ,dados_novos.customer_city
    ,dados_novos.customer_state
  )
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM poc_datum.olist.dim_customers
# MAGIC WHERE customer_id = '2455a94ebab82b39829283f823a69bba'
