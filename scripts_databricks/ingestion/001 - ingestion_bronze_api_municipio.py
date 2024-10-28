# Databricks notebook source
# DBTITLE 1,Importar bibliotecas necessárias
import requests
import json
import pyspark.sql.functions as f

# COMMAND ----------

# DBTITLE 1,conexão com a url da API
api_url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

response = requests.get(api_url)

if response.status_code == 200:
    data = response.json()
else:
    print(f"Erro ao acessar a API: {response.status_code}")
    data = []

# COMMAND ----------

rdd = spark.sparkContext.parallelize(data)

df = rdd.map(lambda entry: {
    'municipio_id': entry['id'],
    'municipio_nome': entry['nome'],
    'microrregiao_id': entry['microrregiao']['id'],
    'microrregiao_nome': entry['microrregiao']['nome'],
    'mesorregiao_id': entry['microrregiao']['mesorregiao']['id'],
    'mesorregiao_nome': entry['microrregiao']['mesorregiao']['nome'],
    'uf_id': entry['microrregiao']['mesorregiao']['UF']['id'],
    'uf_sigla': entry['microrregiao']['mesorregiao']['UF']['sigla'],
    'uf_nome': entry['microrregiao']['mesorregiao']['UF']['nome'],
    'regiao_id': entry['microrregiao']['mesorregiao']['UF']['regiao']['id'],
    'regiao_nome': entry['microrregiao']['mesorregiao']['UF']['regiao']['nome'],
    'regiao_imediata_id': entry['regiao-imediata']['id'],
    'regiao_imediata_nome': entry['regiao-imediata']['nome'],
    'regiao_intermediaria_id': entry['regiao-imediata']['regiao-intermediaria']['id'],
    'regiao_intermediaria_nome': entry['regiao-imediata']['regiao-intermediaria']['nome']
}).toDF()

# COMMAND ----------

df.withColumn(
        "ingestion_date",
        f.current_date()
    )\
    .write\
    .format("delta")\
    .mode("overwrite")\
    .option("path", "/mnt/bronze/tb_municipio")\
    .saveAsTable("bronze.tb_municipio")

# COMMAND ----------

#spark.sql("""SELECT * FROM bronze.tb_municipio""").display()