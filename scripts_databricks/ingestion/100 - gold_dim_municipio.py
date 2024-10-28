# Databricks notebook source
from pyspark.sql import functions as f

# COMMAND ----------

df = spark.sql("""
    MERGE INTO gold.dim_municipio AS gold
    USING silver.tb_municipio AS silver
    ON gold.municipio_id = silver.municipio_id
    WHEN MATCHED AND (
        gold.mesorregiao_id != silver.mesorregiao_id OR
        gold.microrregiao_id != silver.microrregiao_id OR
        gold.regiao_id != silver.regiao_id OR
        gold.uf_id != silver.uf_id
    ) 
    THEN 
        UPDATE SET 
            gold.mesorregiao_id = silver.mesorregiao_id,
            gold.mesorregiao_nome = silver.mesorregiao_nome,
            gold.microrregiao_id = silver.microrregiao_id,
            gold.microrregiao_nome = silver.microrregiao_nome,
            gold.municipio_nome = silver.municipio_nome,
            gold.regiao_id = silver.regiao_id,
            gold.regiao_nome = silver.regiao_nome,
            gold.regiao_imediata_id = silver.regiao_imediata_id,
            gold.regiao_imediata_nome = silver.regiao_imediata_nome,
            gold.regiao_intermediaria_id = silver.regiao_intermediaria_id,
            gold.regiao_intermediaria_nome = silver.regiao_intermediaria_nome,
            gold.uf_id = silver.uf_id,
            gold.uf_nome = silver.uf_nome,
            gold.uf_sigla = silver.uf_sigla,
            gold.ingestion_date = current_date()
    WHEN NOT MATCHED THEN 
        INSERT *
""")

# COMMAND ----------

spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled=false")
spark.sql("VACUUM gold.dim_municipio RETAIN 0 HOURS")

# COMMAND ----------

#spark.sql("""SELECT * FROM gold.dim_municipio""").display()