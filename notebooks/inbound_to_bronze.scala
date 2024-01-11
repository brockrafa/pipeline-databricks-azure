// Databricks notebook source
val path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
val dados = spark.read.json(path)

// COMMAND ----------

val dadosAnuncio = dados.drop("imagens","usuario")

// COMMAND ----------

import org.apache.spark.sql.functions.col

val df_bronze = dadosAnuncio.withColumn("id",col("anuncio.id"))

display(df_bronze)

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"

df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------

display(dbutils.fs.ls("/mnt/dados/bronze/dataset_imoveis"))
