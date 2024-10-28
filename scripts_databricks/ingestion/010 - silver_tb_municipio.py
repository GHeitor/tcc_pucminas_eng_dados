# Databricks notebook source
from pyspark.sql import functions as f

# COMMAND ----------

df = spark.sql("""
    MERGE INTO silver.tb_municipio AS silver
    USING bronze.tb_municipio AS bronze
    ON silver.municipio_id = bronze.municipio_id
    WHEN MATCHED AND (
        silver.mesorregiao_id != bronze.mesorregiao_id OR
        silver.microrregiao_id != bronze.microrregiao_id OR
        silver.regiao_id != bronze.regiao_id OR
        silver.uf_id != bronze.uf_id
    ) 
    THEN 
        UPDATE SET 
            silver.mesorregiao_id = bronze.mesorregiao_id,
            silver.mesorregiao_nome = bronze.mesorregiao_nome,
            silver.microrregiao_id = bronze.microrregiao_id,
            silver.microrregiao_nome = bronze.microrregiao_nome,
            silver.municipio_nome = bronze.municipio_nome,
            silver.regiao_id = bronze.regiao_id,
            silver.regiao_nome = bronze.regiao_nome,
            silver.regiao_imediata_id = bronze.regiao_imediata_id,
            silver.regiao_imediata_nome = bronze.regiao_imediata_nome,
            silver.regiao_intermediaria_id = bronze.regiao_intermediaria_id,
            silver.regiao_intermediaria_nome = bronze.regiao_intermediaria_nome,
            silver.uf_id = bronze.uf_id,
            silver.uf_nome = bronze.uf_nome,
            silver.uf_sigla = bronze.uf_sigla,
            silver.ingestion_date = current_date()
    WHEN NOT MATCHED THEN 
        INSERT *
""")

# COMMAND ----------

#spark.sql("""SELECT * FROM silver.tb_municipio""").display()