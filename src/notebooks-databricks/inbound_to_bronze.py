# Databricks notebook source
# MAGIC %md
# MAGIC Verificar o acesso aos dados em inbound

# COMMAND ----------


dbutils.fs.ls('dbfs:/mnt/dados/inbound')

# COMMAND ----------

# MAGIC %md
# MAGIC Ler os dados inbound

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

path='dbfs:/mnt/dados/inbound/spotify-charts-data.csv'
# Ler CSV com encoding UTF-8
bronze_df= spark.read \
    .option("header", "true") \
    .option("encoding", "UTF-8") \
    .csv(path)

display(bronze_df)

# COMMAND ----------

#Verificar as colunas presentes na tabela
bronze_df.printSchema()

# COMMAND ----------

# Renomear a primeira coluna
id_column = bronze_df.columns[0]
bronze_df = bronze_df.withColumnRenamed(id_column, 'id')
display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Verificar inconsistências

# COMMAND ----------

#listar todas as regiões presentes no dataset
unique_countries = bronze_df.select('region').distinct()

# Mostrar os valores únicos

unique_countries.show(n=100, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Exportando a tabela delta para Azure

# COMMAND ----------

# Desabilitar o format check na configuração
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

# Renomear colunas para remover caracteres invalidos para delta table
unwanted_chars = [' ', ';', '{', '}', '(', ')', '\n', '\t', '=']
substitute_char = '_'

# Renomear colunas para remover caracteres inválidos para delta table
bronze_df = bronze_df.toDF(*[
    c.translate(str.maketrans({char: substitute_char for char in unwanted_chars}))
    for c in bronze_df.columns])

# Salvar a base na camada bronze
output_path = 'dbfs:/mnt/dados/bronze/spotifycharts'
bronze_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(output_path)
