# Databricks notebook source
# MAGIC %md
# MAGIC ### Premissas:
# MAGIC - Nome: dim_calendario
# MAGIC - Descrição da tabela: Tabela composta por variáveis qualitativas de tempo.
# MAGIC - Tipo: SCD-1

# COMMAND ----------

# Importa bibliotecas
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType, LongType
from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

# Funçõe de depara e traduções

# Mapeamento de meses em inglês para português
meses_pt_br = {
    'January': 'Janeiro', 'February': 'Fevereiro', 'March': 'Março',
    'April': 'Abril', 'May': 'Maio', 'June': 'Junho',
    'July': 'Julho', 'August': 'Agosto', 'September': 'Setembro',
    'October': 'Outubro', 'November': 'Novembro', 'December': 'Dezembro'
}

# Função para traduzir o nome do mês
def traduz_mes(mes):
    return meses_pt_br.get(mes, mes)

# ------------------------------------------------------------------------

# Mapeamento de meses em inglês para português abreviados
meses_pt_br_abrv = {
    'Jan': 'Jan', 'Feb': 'Fev', 'Mar': 'Mar',
    'Apr': 'Abr', 'May': 'Mai', 'Jun': 'Jun',
    'Jul': 'Jul', 'Aug': 'Ago', 'Sep': 'Set',
    'Oct': 'Out', 'Nov': 'Nov', 'Dec': 'Dez'
}

# Função para traduzir o nome do mês abreviado
def traduz_mes_abrv(mes):
    return meses_pt_br_abrv.get(mes, mes)

# ------------------------------------------------------------------------

# Mapeamento de meses numéricos para português
meses_pt_br_num = {
    '01': 'Janeiro', '02': 'Fevereiro', '03': 'Março',
    '04': 'Abril', '05': 'Maio', '06': 'Junho',
    '07': 'Julho', '08': 'Agosto', '09': 'Setembro',
    '10': 'Outubro', '11': 'Novembro', '12': 'Dezembro'
}

# Função para traduzir o nome do mês em 'mes_ano'
def traduz_mes_ano(mes_ano):
    mes, ano = mes_ano.split('-')
    mes_traduzido = meses_pt_br_num.get(mes)
    return f"{mes_traduzido}-{ano}"

# ------------------------------------------------------------------------

# Mapeamento dos dias da semana para português
dias_da_semana_pt_br = {
    'Monday': 'Segunda-feira', 'Tuesday': 'Terça-feira', 'Wednesday': 'Quarta-feira',
    'Thursday': 'Quinta-feira', 'Friday': 'Sexta-feira', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
}

def traduz_dia_semana(dia):
    return dias_da_semana_pt_br.get(dia, dia)

# ------------------------------------------------------------------------

# Mapeamento dos dias da semana para português abreviado
dias_da_semana_pt_br_abrv = {
    'Monday': 'Seg', 'Tuesday': 'Ter', 'Wednesday': 'Qua',
    'Thursday': 'Qui', 'Friday': 'Sex', 'Saturday': 'Sab', 'Sunday': 'Dom'
}

def traduz_dia_semana_abrv(dia):
    return dias_da_semana_pt_br_abrv.get(dia, dia)


# ------------------------------------------------------------------------

# Formata trimestre
def formatar_trimestre(trimestre):
    return f"T{trimestre}"

# COMMAND ----------

# Cria Dataframe com dados da dimensão calendário

# Nome da SK
nome_sk = 'sk_dim_calendario'

# Define o intervalo de datas que você deseja para sua tabela de dimensão tempo
dataInicial = to_date(lit("2000-01-01"))            # Converte as strings de data para o tipo Date
dataFinal = to_date(lit(datetime.now().strftime('%Y-%m-%d')))    # Pega a data atual

# Cria uma sequência de datas
df_datas = spark.range(1).select(explode(sequence(dataInicial, dataFinal)).alias('data'))

# Adiciona colunas necessárias
df_calendario = df_datas \
    .withColumn(nome_sk, monotonically_increasing_id()) \
    .withColumn("ano", year(col('data'))) \
    .withColumn("semana_do_ano", dayofweek(col('data'))) \
    .withColumn("dia_do_ano", dayofyear(col('data'))) \
    .withColumn("mes", month(col('data'))) \
    .withColumn("mes_ano", date_format('data','MM-yyyy')) \
    .withColumn("nome_mes_ano", date_format('data','MMM-yyyy')) \
    .withColumn("nome_mes", date_format('data','MMMM')) \
    .withColumn("nome_mes_abrv", date_format('data','MMM')) \
    .withColumn("semestre", when(col("mes") <= 6, "S1").otherwise("S2")) \
    .withColumn("trimestre", quarter(col('data'))) \
    .withColumn("nome_dia_semana", date_format(col('data'), 'EEEE')) \
    .withColumn("nome_dia_semana_abrv", date_format(col('data'), 'EEEE')) \
    .withColumn("dia", dayofmonth(col('data'))) \

# Definição e aplicação das UDFs para tradução dos nomes dos meses
traduz_mes_udf = udf(traduz_mes, StringType())
traduz_mes_abrv_udf = udf(traduz_mes_abrv, StringType())
traduz_mes_ano_udf = udf(traduz_mes_ano, StringType())
traduz_dia_semana_udf = udf(traduz_dia_semana, StringType())
traduz_dia_semana_abrv_udf = udf(traduz_dia_semana_abrv, StringType())
formatar_trimestre_udf = udf(formatar_trimestre, StringType())
    
# Aplicação da UDF para tradução dos nomes dos meses
df_calendario = df_calendario \
    .withColumn("nome_mes", traduz_mes_udf(col("nome_mes"))) \
    .withColumn("nome_mes_abrv", traduz_mes_abrv_udf(col("nome_mes_abrv"))) \
    .withColumn("nome_mes_ano", traduz_mes_ano_udf(col("mes_ano"))) \
    .withColumn("nome_dia_semana", traduz_dia_semana_udf(col("nome_dia_semana"))) \
    .withColumn("nome_dia_semana_abrv", traduz_dia_semana_abrv_udf(col("nome_dia_semana_abrv"))) \
    .withColumn("trimestre", formatar_trimestre_udf(col('trimestre')))

# Exibe o resultado
# display(df_calendario)
df_calendario.show()

# COMMAND ----------

# Cria dataframe para a criação da dimensão
df_dimensao = df_calendario

# Insere as colunas default de dimensão
df_dimensao = df_dimensao.withColumn("row_ingestion_timestamp", current_timestamp()) 

# Ordenando as colunas
df_dimensao_select = df_dimensao.select(
     "sk_dim_calendario"
    ,"row_ingestion_timestamp"
    ,"data" 
    ,"ano" 
    ,"semana_do_ano" 
    ,"dia_do_ano"
    ,"mes" 
    ,"mes_ano" 
    ,"nome_mes_ano" 
    ,"nome_mes" 
    ,"nome_mes_abrv" 
    ,"semestre"  
    ,"trimestre" 
    ,"nome_dia_semana" 
    ,"nome_dia_semana_abrv" 
    ,"dia"
    )

display(df_dimensao_select)

# COMMAND ----------

table_name = 'dim_calendar'
spark.sql('USE olist')
spark.sql(f'DROP TABLE IF EXISTS inter_sales')
df_dimensao_select.write.format("delta").mode('overwrite').saveAsTable(table_name)
