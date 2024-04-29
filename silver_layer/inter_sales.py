# Databricks notebook source
# Importando Bibliotecas
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, NumericType, DateType, BooleanType, TimestampType
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC # Cria tabela intermediária de vendas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW inter_vendas AS
# MAGIC WITH temp_order_items AS (
# MAGIC   SELECT DISTINCT 
# MAGIC     order_id,
# MAGIC     -- order_item_id,
# MAGIC     product_id,
# MAGIC     seller_id,
# MAGIC     shipping_limit_date,
# MAGIC     price,
# MAGIC     freight_value,
# MAGIC     row_ingestion_timestamp
# MAGIC   FROM poc_datum.olist.order_items_silver
# MAGIC   WHERE 1 =1 
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC  o.order_id	
# MAGIC ,o.customer_id	
# MAGIC ,o.order_status	
# MAGIC ,o.order_purchase_timestamp	
# MAGIC ,o.order_approved_at	
# MAGIC ,o.order_delivered_carrier_date	
# MAGIC ,o.order_delivered_customer_date	
# MAGIC ,o.order_estimated_delivery_date
# MAGIC ,o.row_ingestion_timestamp
# MAGIC ,oi.product_id	
# MAGIC ,oi.seller_id	
# MAGIC ,oi.shipping_limit_date	
# MAGIC ,oi.price	
# MAGIC ,oi.freight_value	
# MAGIC FROM poc_datum.olist.order_silver AS o
# MAGIC INNER JOIN temp_order_items as oi
# MAGIC   ON o.order_id = oi.order_id
# MAGIC -- WHERE o.order_id = 'c642b243037172b9fa789eec22ab713c'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM inter_vendas

# COMMAND ----------

# Seleciona o banco de dados 'olist' para uso
table_name = 'inter_sales'
spark.sql('USE olist')
spark.sql(f'DROP TABLE IF EXISTS inter_sales')

df = spark.sql('SELECT * FROM inter_vendas')

# Grava o DataFrame no formato Parquet
destination_path = f'dbfs:/FileStore/datum/silver/olistbr-brazilian-ecommerce/{table_name}'
df.write.mode('overwrite').parquet(destination_path)

df.write.format("delta").mode('overwrite').saveAsTable(table_name)
