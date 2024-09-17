# Databricks notebook source
# MAGIC %md
# MAGIC Verificar a tabela 

# COMMAND ----------

from pyspark.sql.types import IntegerType, DateType, BooleanType, FloatType

# COMMAND ----------

#Carregar o delta em DataFrame
path = "dbfs:/mnt/dados/bronze/spotifycharts"
silver_df = spark.read.format("delta").load(path, inferSchema=True)
display(silver_df)

# COMMAND ----------

# Informações da tabela antes das transformações

# Obter o número de linhas
rows = silver_df.count()

# Obter o número de colunas
columns = len(silver_df.columns)

# Obter o tamanho em bytes
bytes_size = silver_df.rdd.map(lambda row: len(str(row))).glom().map(sum).sum()
mb_size = bytes_size/(1024**2)
gb_size = bytes_size / (1024 ** 3)


print(f'Número de linhas: {rows}')
print(f'Número de colunas: {columns}')
print(f'Tamanho em MB: {mb_size:.2f} MB')
print(f'Tamanho em GB: {gb_size:.2f} GB')

# COMMAND ----------

#Obter as informações das colunas
silver_df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC Remover colunas que não serão utilizadas na análise

# COMMAND ----------

#Remover colunas que não serão utilizadas na análise de dados do projeto
columns_to_drop = columns = ["af_key",  "af_mode", "af_speechiness", "af_acousticness", "af_instrumentalness", "af_liveness", "af_valence", "af_tempo", "af_time_signature"]

silver_df = silver_df.drop(*columns_to_drop)
display(silver_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Remover regiões que não serão utilizadas na análise

# COMMAND ----------

# Verificar quais países estão presentes no dataset
silver_df.select('region').distinct().show()


# COMMAND ----------

#Selecionar a regiãao global e países que falam português, inglês e espanhol
regions = ["Argentina", "United States", "Chile", "Bolivia", "Spain", "Mexico", "Brazil", "Colombia", "United Kingdom", "Global"]
silver_df = silver_df.filter(silver_df.region.isin([*regions]))
display(silver_df)


# COMMAND ----------

#Obter as informações das colunas
silver_df.printSchema()

# COMMAND ----------

# Informações da tabela silver antes da definição de tipos

# Obter o número de linhas
rows = silver_df.count()

# Obter o número de colunas
columns = len(silver_df.columns)

# Obter o tamanho em bytes
bytes_size = silver_df.rdd.map(lambda row: len(str(row))).glom().map(sum).sum()
mb_size = bytes_size/(1024**2)
gb_size = bytes_size / (1024 ** 3)


print(f'Número de linhas: {rows}')
print(f'Número de colunas: {columns}')
print(f'Tamanho em MB: {mb_size:.2f} MB')
print(f'Tamanho em GB: {gb_size:.2f} GB')

# COMMAND ----------


# Definir o tipo das colunas

# Definir a função para alterar os tipos das colunas
def change_column_types(df, column, data_type):
    return df.withColumn(column, df[column].cast(data_type))

# Dicionário mapeando nomes de colunas para seus tipos de dados desejados
columns_to_cast = {
    'rank': IntegerType(),
    'date': DateType(),
    'streams': IntegerType(),
    'popularity': IntegerType(),
    'duration_ms': IntegerType(),
    'explicit': BooleanType(),
    'release_date': DateType(),
    'af_danceability': FloatType(),
    'af_energy': FloatType(),
    'af_loudness': FloatType()
}

# Aplicar as transformações
for column, data_type in columns_to_cast.items():
    silver_df = change_column_types(silver_df, column, data_type)

silver_df.printSchema()




# COMMAND ----------

# Informações da tabela silver depois da definição de tipos

# Obter o número de linhas
rows = silver_df.count()

# Obter o número de colunas
columns = len(silver_df.columns)

# Obter o tamanho em bytes
bytes_size = silver_df.rdd.map(lambda row: len(str(row))).glom().map(sum).sum()
mb_size = bytes_size/(1024**2)
gb_size = bytes_size / (1024 ** 3)


print(f'Número de linhas: {rows}')
print(f'Número de colunas: {columns}')
print(f'Tamanho em MB: {mb_size:.2f} MB')
print(f'Tamanho em GB: {gb_size:.2f} GB')

# COMMAND ----------

# MAGIC %md
# MAGIC Exportando a tabela para a camada silver

# COMMAND ----------



# Renomear colunas para remover caracteres invalidos para delta table
unwanted_chars = [' ', ';', '{', '}', '(', ')', '\n', '\t', '=']
substitute_char = '_'

# Renomear colunas para remover caracteres inválidos para delta table
silver_df = silver_df.toDF(*[
    c.translate(str.maketrans({char: substitute_char for char in unwanted_chars}))
    for c in silver_df.columns])

# Salvar a base na camada bronze
output_path = 'dbfs:/mnt/dados/silver/spotifycharts'
silver_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(output_path)
