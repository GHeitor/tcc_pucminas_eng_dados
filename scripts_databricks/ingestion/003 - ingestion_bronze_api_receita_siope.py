# Databricks notebook source
# DBTITLE 1,Importar bibliotecas necessárias
import requests
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# COMMAND ----------

dbutils.widgets.text("Ano_Consulta", "2024", "Ano de Consulta")
dbutils.widgets.text("Num_Peri", "1", "Número do Período")
dbutils.widgets.text("Sig_UF", "RJ", "UF")

ano_consulta = dbutils.widgets.get("Ano_Consulta")
num_peri = dbutils.widgets.get("Num_Peri")
sig_uf = dbutils.widgets.get("Sig_UF")

# COMMAND ----------

api_url = f"https://www.fnde.gov.br/olinda-ide/servico/DADOS_ABERTOS_SIOPE/versao/v1/odata/Receita_Siope(Ano_Consulta=@Ano_Consulta,Num_Peri=@Num_Peri,Sig_UF=@Sig_UF)?@Ano_Consulta={ano_consulta}&@Num_Peri={num_peri}&@Sig_UF='{sig_uf}'&$format=json"

response = requests.get(api_url)

if response.status_code == 200:
    json_data = response.json()
    data = json_data.get("value", [])
else:
    raise Exception(f"Erro ao acessar a API: {response.status_code}")

# COMMAND ----------

if data:
    # Definir um esquema customizado para expandir corretamente o JSON
    schema = StructType([
        StructField("TIPO", StringType(), True),
        StructField("NUM_ANO", StringType(), True),
        StructField("NUM_PERI", StringType(), True),
        StructField("COD_UF", StringType(), True),
        StructField("SIG_UF", StringType(), True),
        StructField("COD_MUNI", StringType(), True),
        StructField("NOM_MUNI", StringType(), True),
        StructField("COD_EXIB_FORMATADO", StringType(), True),
        StructField("NOM_ITEM", StringType(), True),
        StructField("IDN_CLAS", StringType(), True),
        StructField("NOM_COLU", StringType(), True),
        StructField("NUM_NIVE", StringType(), True),
        StructField("NUM_ORDE", StringType(), True),
        StructField("VAL_DECL", StringType(), True)
    ])
    
    # Criar um DataFrame PySpark a partir da lista de dicionários (que está em 'value') usando o esquema
    df_json = spark.createDataFrame(data, schema=schema)
    
    # Adicionar a coluna 'ingestion_date' com a data atual
    df = df_json\
        .select([f.col(col_name).alias(col_name.lower()) for col_name in df_json.columns]
        )\
        .withColumn(
            "ingestion_date",
            f.current_date()
        )\
        .write\
        .format("delta")\
        .mode("overwrite")\
        .option("path", "/mnt/bronze/tb_receita_siope")\
        .saveAsTable("bronze.tb_receita_siope")

    # Salvar os dados na tabela Delta na camada bronze
else:
    raise Exception("Nenhum dado encontrado no campo 'value' do JSON.")

# COMMAND ----------

#spark.sql("""SELECT * FROM bronze.tb_receita_siope""").display()