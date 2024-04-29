# Databricks notebook source
# MAGIC %md
# MAGIC # Analise de Dados

# COMMAND ----------

# MAGIC %md
# MAGIC **Premissa**: Execute consultas exploratórias para entender melhor os dados e validar as transformações.
# MAGIC Crie visualizações ou relatórios para comunicar insights.

# COMMAND ----------

# MAGIC %md
# MAGIC # Tabelas
# MAGIC
# MAGIC   - **olist_orders_dataset** : Tabela principal, usada para conectar todos os detalhes relacionados a um pedido de compra.
# MAGIC   - **olist_order_items_dataset** : Contém os detalhes de um item que foi comprado.
# MAGIC   - **olist_order_reviews_dataset** : Contém detalhes relacionados a quaisquer comentários postados pelo cliente sobre um produto.
# MAGIC   - **olist_products_dataset** : Contém informações relacionadas a um produto.
# MAGIC   - **olist_order_payments_dataset** : Contém informações relacionadas aos detalhes de pagamento.
# MAGIC   - **olist_customers_dataset** : Contém informações de clientes.
# MAGIC   - **olist_geolocation_dataset** : Contém informações geográficas relacionadas aos vendedores e clientes.
# MAGIC   - **olist_sellers_dataset** : Contém informações relativas a todos os vendedores que se cadastraram nesta empresa.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Quantidade de pedidos
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_pedidos_entregues
# MAGIC     FROM poc_datum.olist.order_silver

# COMMAND ----------

# MAGIC %md
# MAGIC #### Quantidade de pedidos por Status

# COMMAND ----------

# DBTITLE 1,Quantidade de pedidos por Status
# MAGIC %sql
# MAGIC WITH total_pedidos_entregues 
# MAGIC AS (
# MAGIC     SELECT 
# MAGIC     COUNT(*) AS total_pedidos_entregues
# MAGIC     FROM poc_datum.olist.order_silver
# MAGIC     WHERE order_status = 'delivered'
# MAGIC ),
# MAGIC total_pedidos_n_entregue 
# MAGIC AS (
# MAGIC     SELECT 
# MAGIC     COUNT(*) AS total_pedidos_n_entregues
# MAGIC     FROM poc_datum.olist.order_silver
# MAGIC     WHERE order_status <> 'delivered'
# MAGIC )
# MAGIC
# MAGIC SELECT 'Delivery' AS status, tpe.total_pedidos_entregues FROM  total_pedidos_entregues AS tpe
# MAGIC UNION
# MAGIC SELECT 'No Delivery' AS status, tpne.total_pedidos_n_entregues FROM  total_pedidos_n_entregue AS tpne
# MAGIC
# MAGIC

# COMMAND ----------

total_pedidos_n_entregue = spark.sql("""
                                    SELECT 
                                        COUNT(*) AS total_pedidos_n_entregues
                                    FROM poc_datum.olist.order_silver
                                        WHERE order_status <> 'delivered'
                                    """).collect()[0][0]

total_pedidos = spark.sql("""
                            SELECT 
                                COUNT(*) AS total_pedidos
                            FROM poc_datum.olist.order_silver
                            """).collect()[0][0]

percent_no_delivery = total_pedidos_n_entregue / total_pedidos

print("Percentual de pedidos não entregues: {:.2%}".format(percent_no_delivery))

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **Eficiência nas Entregas**: A diferença entre as barras é bastante acentuada, com a barra de entregas muito maior que a de não entregues. Isso sugere que a maioria dos pedidos é concluída com sucesso.
# MAGIC
# MAGIC 2. **Pedidos Não Entregues**: Apesar que a quantidade seja significativamente menor (aprox. **2,98%**), ainda representa uma quantidade de pedidos que não chegaram ao destino final. É importante entender os motivos por trás desses casos para melhorar a eficácia da entrega.

# COMMAND ----------

# DBTITLE 1,Quantidade de pedidos não entregues
# MAGIC %sql
# MAGIC WITH total_pedidos AS (
# MAGIC     SELECT 
# MAGIC     COUNT(*) AS total_geral_pedidos
# MAGIC     FROM poc_datum.olist.order_silver
# MAGIC     WHERE order_status <> 'delivered'
# MAGIC ),
# MAGIC
# MAGIC total_pedidos_n_entregue AS (
# MAGIC     SELECT 
# MAGIC     DISTINCT
# MAGIC     order_status,
# MAGIC     COUNT(*) AS total_pedidos
# MAGIC     FROM poc_datum.olist.order_silver
# MAGIC     WHERE order_status <> 'delivered'
# MAGIC     GROUP BY order_status
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC     s.order_status,
# MAGIC     s.total_pedidos,
# MAGIC     (s.total_pedidos * 100.0 / t.total_geral_pedidos) AS percentual
# MAGIC FROM 
# MAGIC     total_pedidos_n_entregue s,
# MAGIC     total_pedidos t
# MAGIC ORDER BY 
# MAGIC     s.total_pedidos DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 1. **Maior Categoria**: A maior barra representa pedidos que foram enviados ("shipped"), indicando que esta é a etapa mais comum atualmente no status dos pedidos. Isso é esperado, já que em qualquer momento, muitos pedidos estarão em trânsito.
# MAGIC
# MAGIC 2. **Pedidos Cancelados**: A segunda maior barra indica uma quantidade significativa de pedidos cancelados. Isso pode sinalizar uma área que merece investigação, pois altas taxas de cancelamento podem indicar problemas no processo de compra, insatisfação do cliente ou problemas de estoque.
# MAGIC
# MAGIC 3. **Disponibilidade**: A terceira maior barra, "unavailable", sugere que muitos produtos não estão disponíveis. Isso pode ser um ponto de atenção para a gestão de estoque, podendo afetar a satisfação dos clientes e marketing negativo por falta de produtos.
# MAGIC
# MAGIC 4. **Processamento e Faturamento**: As barras para "processing" e "invoiced" mostram números menores. Isso aparentemente é normal, pois representam etapas transitórias rápidas no ciclo de vida de um pedido. Se esses números fossem anormalmente altos, poderia indicar gargalos no processamento de pedidos ou na faturamento da nota.
# MAGIC
# MAGIC 5. **Criação e Aprovação**: Existem muito poucos pedidos nas etapas iniciais de "created" e "approved". Isso pode indicar que os pedidos estão sendo processados rapidamente através dessas etapas iniciais, o que geralmente é um bom sinal de eficiência.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Evolução ao longo do tempo

# COMMAND ----------

# DBTITLE 1,Evolução de Pedidos ao mês/ano
# MAGIC %sql
# MAGIC WITH total_pedidos 
# MAGIC AS (
# MAGIC SELECT  
# MAGIC   date_format(ops.order_purchase_timestamp, 'yyyy-MM' ) AS mes_ano ,*
# MAGIC FROM 
# MAGIC   poc_datum.olist.order_silver AS ops )
# MAGIC
# MAGIC SELECT
# MAGIC   mes_ano,
# MAGIC   count(*) as total_pedidos
# MAGIC FROM total_pedidos
# MAGIC GROUP BY mes_ano
# MAGIC ORDER BY 1 

# COMMAND ----------

# DBTITLE 1,Evolução de Pedidos ao longo do tempo
# MAGIC %sql
# MAGIC WITH total_pedidos 
# MAGIC AS (
# MAGIC SELECT  
# MAGIC   date_format(ops.order_purchase_timestamp, 'yyyy-MM-dd' ) AS dia ,*
# MAGIC FROM 
# MAGIC   poc_datum.olist.order_silver AS ops )
# MAGIC
# MAGIC SELECT
# MAGIC   dia,
# MAGIC   count(*) as total_pedidos
# MAGIC FROM total_pedidos
# MAGIC GROUP BY dia
# MAGIC ORDER BY 1 

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **Crescimento ao Longo do Tempo**: Existe uma tendência geral de crescimento no volume de pedidos ao longo do tempo, indicando um aumento na atividade de vendas ou na aquisição de clientes.
# MAGIC
# MAGIC 2. **Flutuações Sazonais**: Parece haver uma sazonalidade nas vendas, com picos e vales que podem corresponder a períodos específicos do ano, como feriados, eventos sazonais ou promoções.
# MAGIC
# MAGIC 3. **Pico de Vendas**: Houve um pico de vendas com mais de 1.000 pedidos em 24 de novembro de 2017, em pesquisas, nota-se que foi em função da Blackfriday.
# MAGIC
# MAGIC 4. **Queda Recente**: Há uma queda acentuada no número de pedidos no ponto mais recente do gráfico. Isso pode ser devido à dados incompletos para o mês mais recente.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC date_format(order_purchase_timestamp, 'HH') AS hora,
# MAGIC count(*) as total_pedidos
# MAGIC FROM poc_datum.olist.order_silver
# MAGIC GROUP BY ALL
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **Horário de Pico**: Há um pico evidente à partir das 8h, que é crescente até o horário de almoço (por volta das 11h até as 13h) e no meio da tarde se mantem até as 22h. Isso sugere que as pessoas podem estar fazendo compras durante o horário de trabalho ou aproveitando as pausas para realizar pedidos. Também pode indicar que o comportamento de compra online é consistente ao longo da tarde e início da noite, o que pode refletir o tempo livre que os consumidores têm após o trabalho para atividades de lazer, como compras online
# MAGIC
# MAGIC 2. **Horas da Madrugada**: As vendas caem significativamente após a meia-noite e começam a aumentar novamente após as 6h. Isso é consistente com os padrões de sono e atividade da maioria das pessoas.
# MAGIC
# MAGIC 3. **Manhã vs. Noite**: As vendas parecem ser mais altas à tarde e à noite em comparação com as horas da manhã. Isso sugere oportunidade em campanhas de marketing e promoções nesse período.
# MAGIC
# MAGIC 4. **Menor Atividade**: O período entre 3h e 5h da manhã tem o menor número de pedidos, o que é típico, considerando que a maioria das pessoas está dormindo.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Modalidade de pagamento

# COMMAND ----------

# DBTITLE 1,Total de Pedidos por Modalidade de Pagamento
# MAGIC %sql
# MAGIC SELECT DISTINCT 
# MAGIC   payment_type, 
# MAGIC   count(*) as total_pedidos 
# MAGIC FROM 
# MAGIC   poc_datum.olist.order_payments_silver
# MAGIC GROUP BY payment_type
# MAGIC ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **Cartão de Crédito**: É o método mais popular, com a maior barra no gráfico, indicando que a grande maioria dos clientes prefere pagar com cartão de crédito. Isso pode refletir a conveniência do pagamento a prazo, a segurança, a acumulação de pontos de recompensa, ou a possibilidade de financiamento das compras.
# MAGIC
# MAGIC 2. **Boleto**: O segundo método mais comum é o boleto bancário. Esta preferência pode estar ligada à popularidade do boleto no Brasil como método de pagamento confiável e amplamente aceito, ou ao fato de ser uma opção para quem não possui cartão de crédito ou não deseja usá-lo online.
# MAGIC
# MAGIC 3. **Voucher**: Os vouchers são a terceira forma de pagamento mais utilizada, o que pode indicar promoções, programas de fidelidade ou a utilização de créditos de devoluções anteriores.
# MAGIC
# MAGIC 4. **Cartão de Débito**: O cartão de débito aparece em menor medida, o que pode sugerir uma preferência menor por pagamentos imediatos ou talvez menos promoções ou benefícios associados a esta forma de pagamento em comparação com cartões de crédito.
# MAGIC
# MAGIC 5. **Não Definido**: Há um número muito pequeno de transações categorizadas como "não definido", que poderia ser devido a erros de processamento ou categorização.
