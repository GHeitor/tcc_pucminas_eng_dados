-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS bronze

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS silver

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS gold

-- COMMAND ----------

SHOW CREATE TABLE silver.tb_municipio

-- COMMAND ----------

CREATE TABLE spark_catalog.bronze.tb_municipio (
  mesorregiao_id BIGINT,
  mesorregiao_nome STRING,
  microrregiao_id BIGINT,
  microrregiao_nome STRING,
  municipio_id BIGINT,
  municipio_nome STRING,
  regiao_id BIGINT,
  regiao_imediata_id BIGINT,
  regiao_imediata_nome STRING,
  regiao_intermediaria_id BIGINT,
  regiao_intermediaria_nome STRING,
  regiao_nome STRING,
  uf_id BIGINT,
  uf_nome STRING,
  uf_sigla STRING,
  ingestion_date DATE)
USING delta
LOCATION 'dbfs:/mnt/bronze/tb_municipio'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')

-- COMMAND ----------

CREATE TABLE spark_catalog.silver.tb_municipio (
  mesorregiao_id BIGINT,
  mesorregiao_nome STRING,
  microrregiao_id BIGINT,
  microrregiao_nome STRING,
  municipio_id BIGINT,
  municipio_nome STRING,
  regiao_id BIGINT,
  regiao_imediata_id BIGINT,
  regiao_imediata_nome STRING,
  regiao_intermediaria_id BIGINT,
  regiao_intermediaria_nome STRING,
  regiao_nome STRING,
  uf_id BIGINT,
  uf_nome STRING,
  uf_sigla STRING,
  ingestion_date DATE)
USING delta
LOCATION 'dbfs:/mnt/silver/tb_municipio'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')

-- COMMAND ----------

CREATE TABLE gold.dim_municipio (
  mesorregiao_id BIGINT,
  mesorregiao_nome STRING,
  microrregiao_id BIGINT,
  microrregiao_nome STRING,
  municipio_id BIGINT,
  municipio_nome STRING,
  regiao_id BIGINT,
  regiao_imediata_id BIGINT,
  regiao_imediata_nome STRING,
  regiao_intermediaria_id BIGINT,
  regiao_intermediaria_nome STRING,
  regiao_nome STRING,
  uf_id BIGINT,
  uf_nome STRING,
  uf_sigla STRING,
  ingestion_date DATE)
USING delta
LOCATION 'dbfs:/mnt/gold/dim_municipio'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')

-- COMMAND ----------

SHOW CREATE TABLE silver.tb_receita_siope

-- COMMAND ----------

CREATE TABLE spark_catalog.bronze.tb_despesa_siope (
  tipo STRING,
  num_ano STRING,
  num_peri STRING,
  cod_uf STRING,
  sig_uf STRING,
  cod_muni STRING,
  nom_muni STRING,
  nom_past STRING,
  idn_exib_codi STRING,
  cod_past STRING,
  cod_subf STRING,
  tip_pasta STRING,
  cod_exib STRING,
  cod_exib_formatado STRING,
  cod_fonte STRING,
  nom_item STRING,
  idn_clas STRING,
  nom_colu STRING,
  num_nive STRING,
  num_orde STRING,
  val_decl STRING,
  ingestion_date DATE)
USING delta
LOCATION 'dbfs:/mnt/bronze/tb_despesa_siope'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')


-- COMMAND ----------

CREATE TABLE spark_catalog.silver.tb_despesa_siope (
  tipo STRING,
  num_ano STRING,
  num_peri STRING,
  cod_uf STRING,
  sig_uf STRING,
  cod_muni STRING,
  nom_muni STRING,
  nom_past STRING,
  idn_exib_codi STRING,
  cod_past STRING,
  cod_subf STRING,
  tip_pasta STRING,
  cod_exib STRING,
  cod_exib_formatado STRING,
  cod_fonte STRING,
  nom_item STRING,
  idn_clas STRING,
  nom_colu STRING,
  num_nive STRING,
  num_orde STRING,
  val_decl DECIMAL(38,2),
  ingestion_date DATE)
USING delta
LOCATION 'dbfs:/mnt/silver/tb_despesa_siope'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')


-- COMMAND ----------

CREATE TABLE spark_catalog.bronze.tb_receita_siope (
  tipo STRING,
  num_ano STRING,
  num_peri STRING,
  cod_uf STRING,
  sig_uf STRING,
  cod_muni STRING,
  nom_muni STRING,
  cod_exib_formatado STRING,
  nom_item STRING,
  idn_clas STRING,
  nom_colu STRING,
  num_nive STRING,
  num_orde STRING,
  val_decl STRING,
  ingestion_date DATE)
USING delta
LOCATION 'dbfs:/mnt/bronze/tb_receita_siope'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')


-- COMMAND ----------

CREATE TABLE spark_catalog.silver.tb_receita_siope (
  tipo STRING,
  num_ano STRING,
  num_peri STRING,
  cod_uf STRING,
  sig_uf STRING,
  cod_muni STRING,
  nom_muni STRING,
  cod_exib_formatado STRING,
  nom_item STRING,
  idn_clas STRING,
  nom_colu STRING,
  num_nive STRING,
  num_orde STRING,
  val_decl DECIMAL(38,2),
  ingestion_date DATE)
USING delta
LOCATION 'dbfs:/mnt/silver/tb_receita_siope'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')


-- COMMAND ----------

CREATE TABLE spark_catalog.gold.fact_evolucao_financeira_siope (
  periodo DATE,
  tipo STRING,
  cod_uf STRING,
  cod_muni STRING,
  cod_exib_formatado STRING,
  nom_item STRING,
  idn_clas STRING,
  nom_colu STRING,
  num_nive STRING,
  num_orde STRING,
  val_decl DECIMAL(38,2),
  ingestion_date DATE)
USING delta
LOCATION 'dbfs:/mnt/gold/fact_evolucao_financeira_siope'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
