# Databricks notebook source
from pyspark.sql import functions as f

# COMMAND ----------

df_silver_receita = spark.table("silver.tb_receita_siope")\
    .withColumn(
        "periodo", 
        f.to_date(
            f.concat(
                f.col("num_ano"),
                f.lit("-"),
                f.lpad(f.col("num_peri"), 2, '0'),
                f.lit("-01")
            ),
             "yyyy-MM-dd"
        )
    )\
    .withColumn(
        "tipo",
        f.lit("Receita")
    )

ult_ingest_receita = df_silver_receita.agg(f.max("ingestion_date")).collect()[0][0]

df_silver_receita = df_silver_receita.filter(f.col("ingestion_date") == ult_ingest_receita)
    
df_silver_despesa = spark.table("silver.tb_despesa_siope")\
    .withColumn(
        "periodo", 
        f.to_date(
            f.concat(
                f.col("num_ano"),
                f.lit("-"),
                f.lpad(f.col("num_peri"), 2, '0'),
                f.lit("-01")
            ),
             "yyyy-MM-dd"
        )
    )\
    .withColumn(
        "tipo",
        f.lit("Despesa")
    )\
    .withColumn(
        "val_decl", 
        f.col("val_decl") * -1
    )

ult_ingest_despesa = df_silver_despesa.agg(f.max("ingestion_date")).collect()[0][0]

df_silver_despesa = df_silver_despesa.filter(f.col("ingestion_date") == ult_ingest_despesa)

df_silver =  df_silver_despesa.unionByName(df_silver_receita, allowMissingColumns=True)

# Identificar todos os anos e períodos únicos presentes na silver
ano_mes_insert = df_silver.select("periodo").distinct()

# Iterar sobre cada combinação de ano e período para realizar o delete e insert
for row in ano_mes_insert.collect():
    ano_mes = row["periodo"]

    spark.sql(f"""
    DELETE FROM gold.fact_evolucao_financeira_siope
    WHERE periodo = '{ano_mes}'
    """)

df_gold = df_silver.select(
        "periodo",
        "tipo",
        "cod_uf",
        "cod_muni",
        "cod_exib_formatado",
        "nom_item",
        "idn_clas",
        "nom_colu",
        "num_nive",
        "num_orde",
        (f.col("val_decl")/1000).cast('decimal(38,2)').alias('val_decl'),
        f.current_date().alias('ingestion_date')
    )\
    .write\
    .format("delta")\
    .mode("append")\
    .option("path", "/mnt/gold/fact_evolucao_financeira_siope")\
    .saveAsTable("gold.fact_evolucao_financeira_siope")

# COMMAND ----------

spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled=false")
spark.sql("VACUUM gold.fact_evolucao_financeira_siope RETAIN 0 HOURS")

# COMMAND ----------

#spark.sql("""SELECT * FROM gold.fact_evolucao_financeira_siope""").display()