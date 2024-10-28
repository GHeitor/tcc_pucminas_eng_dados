# Databricks notebook source
from pyspark.sql import functions as f

# COMMAND ----------

df_bronze = spark.table("bronze.tb_despesa_siope")

# Identificar todos os anos e períodos únicos presentes na Bronze
ano_mes_insert = df_bronze.select("num_ano", "num_peri").distinct()

# Iterar sobre cada combinação de ano e período para realizar o delete e insert
for row in ano_mes_insert.collect():
    ano_consulta = row["num_ano"]
    periodo_consulta = row["num_peri"]

    spark.sql(f"""
    DELETE FROM silver.tb_despesa_siope
    WHERE num_ano = {ano_consulta} AND num_peri = {periodo_consulta}
    """)
    
df_bronze.withColumn(
        "ingestion_date",
        f.current_date()
    )\
    .withColumn(
        "val_decl",
        (f.col("val_decl")/1000).cast("decimal(38,2)")
    )\
    .write\
    .format("delta")\
    .mode("append")\
    .option("path", "/mnt/silver/tb_despesa_siope")\
    .saveAsTable("silver.tb_despesa_siope")

# COMMAND ----------

#spark.sql("""SELECT * FROM silver.tb_despesa_siope""").display()