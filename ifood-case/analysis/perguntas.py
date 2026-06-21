# Databricks notebook source
# MAGIC %md
# MAGIC # Análises - NYC Yellow Taxi (Jan-Mai 2023)
# MAGIC
# MAGIC Respostas para as perguntas do case, usando a tabela de consumo
# MAGIC `silver.yellow_tripdata`. Cada pergunta é respondida em PySpark
# MAGIC (DataFrame API) e replicada em Spark SQL, evidenciando que a tabela
# MAGIC pode ser livremente consumida via SQL pelos usuários finais.

# COMMAND ----------

import os
import sys
from pyspark.sql import SparkSession, functions as F

sys.path.append("/Workspace/Repos/ifood/ifood-01/ifood-case")
from src.utils.config import IS_DATABRICKS, SILVER_TABLE

# COMMAND ----------

def get_spark() -> SparkSession:
    if IS_DATABRICKS:
        return SparkSession.builder.getOrCreate()
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("ifood-case-analysis")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
df = spark.table(SILVER_TABLE)
print(f"Total de registros na silver: {df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1
# MAGIC **Qual a média de valor total (`total_amount`) recebido em um mês,
# MAGIC considerando todos os yellow táxis da frota?**
# MAGIC
# MAGIC Interpretação: média mensal do `total_amount`, agrupando as corridas
# MAGIC pelo mês (`trip_year`/`trip_month`) em que ocorreram.

# COMMAND ----------

# PySpark
resp1_df = (
    df.groupBy("trip_year", "trip_month")
    .agg(
        F.round(F.avg("total_amount"), 2).alias("media_total_amount"),
        F.count("*").alias("qtd_corridas"),
    )
    .orderBy("trip_year", "trip_month")
)
resp1_df.show()

# COMMAND ----------

# Spark SQL equivalente
df.createOrReplaceTempView("yellow_tripdata")

spark.sql(
    """
    SELECT
        trip_year,
        trip_month,
        ROUND(AVG(total_amount), 2) AS media_total_amount,
        COUNT(*) AS qtd_corridas
    FROM yellow_tripdata
    GROUP BY trip_year, trip_month
    ORDER BY trip_year, trip_month
    """
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2
# MAGIC **Qual a média de passageiros (`passenger_count`) por cada hora do
# MAGIC dia que pegaram táxi no mês de maio, considerando todos os táxis da
# MAGIC frota?**
# MAGIC
# MAGIC Interpretação: para cada hora do dia (0–23) de maio/2023, a média de
# MAGIC `passenger_count` das corridas iniciadas naquela hora.

# COMMAND ----------

# PySpark
resp2_df = (
    df.filter((F.col("trip_year") == 2023) & (F.col("trip_month") == 5))
    .groupBy("trip_hour")
    .agg(
        F.round(F.avg("passenger_count"), 2).alias("media_passenger_count"),
        F.count("*").alias("qtd_corridas"),
    )
    .orderBy("trip_hour")
)
resp2_df.show(24)

# COMMAND ----------

# Spark SQL equivalente
spark.sql(
    """
    SELECT
        trip_hour,
        ROUND(AVG(passenger_count), 2) AS media_passenger_count,
        COUNT(*) AS qtd_corridas
    FROM yellow_tripdata
    WHERE trip_year = 2023 AND trip_month = 5
    GROUP BY trip_hour
    ORDER BY trip_hour
    """
).show(24)

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Opcional) Persistindo os resultados em uma camada Gold
# MAGIC Útil para consumo direto por BI/dashboards, sem reprocessar a silver.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

resp1_df.write.format("delta").mode("overwrite").saveAsTable("gold.media_total_amount_por_mes")
resp2_df.write.format("delta").mode("overwrite").saveAsTable("gold.media_passageiros_por_hora_maio")

print("Tabelas gold gravadas: gold.media_total_amount_por_mes, gold.media_passageiros_por_hora_maio")
