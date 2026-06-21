# Databricks notebook source
# MAGIC %md
# MAGIC # Transformação - Bronze -> Silver (Camada de Consumo)
# MAGIC
# MAGIC Lê a tabela `bronze.yellow_tripdata`, aplica limpeza e seleciona as
# MAGIC colunas exigidas pelo desafio, gravando a tabela Delta de consumo
# MAGIC `silver.yellow_tripdata`, particionada por ano/mês da corrida, pronta
# MAGIC para ser consultada via SQL pelos usuários finais.

# COMMAND ----------

import os
import sys
from pyspark.sql import SparkSession, functions as F

repo_root = "/Workspace/Repos/ifood/ifood-01/ifood-case"
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from src.utils.config import (
    IS_DATABRICKS,
    BRONZE_TABLE,
    SILVER_PATH,
    SILVER_DB,
    SILVER_TABLE,
    REQUIRED_COLUMNS,
)

# COMMAND ----------

def get_spark() -> SparkSession:
    if IS_DATABRICKS:
        return SparkSession.builder.getOrCreate()
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("ifood-case-bronze-to-silver")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

# COMMAND ----------

def run():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = spark.table(BRONZE_TABLE)
    print(f"Total de registros na bronze: {bronze_df.count():,}")

    # 1) Seleciona apenas as colunas exigidas pelo desafio
    #    (as demais colunas originais podem ser ignoradas na camada de consumo)
    silver_df = bronze_df.select(*REQUIRED_COLUMNS)

    # 2) Tipagem explícita, garantindo consistência mesmo se a fonte mudar tipos
    silver_df = (
        silver_df.withColumn("VendorID", F.col("VendorID").cast("int"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
    )

    # 3) Colunas derivadas, úteis para particionamento e para as análises
    silver_df = (
        silver_df.withColumn("trip_year", F.year("tpep_pickup_datetime"))
        .withColumn("trip_month", F.month("tpep_pickup_datetime"))
        .withColumn("trip_hour", F.hour("tpep_pickup_datetime"))
    )

    # 4) Regras de qualidade de dados
    rows_before = silver_df.count()

    silver_df = (
        silver_df
        # remove duplicatas exatas (proteção contra reprocessamento)
        .dropDuplicates()
        # passenger_count precisa existir e ser > 0 para fazer sentido em média de passageiros
        .filter(F.col("passenger_count").isNotNull() & (F.col("passenger_count") > 0))
        # total_amount negativo geralmente é estorno/ajuste, não uma corrida válida
        .filter(F.col("total_amount").isNotNull() & (F.col("total_amount") >= 0))
        # datas nulas ou inconsistentes (dropoff antes do pickup) são descartadas
        .filter(
            F.col("tpep_pickup_datetime").isNotNull()
            & F.col("tpep_dropoff_datetime").isNotNull()
            & (F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))
        )
        # mantém apenas o período do desafio (Jan-Mai/2023), removendo eventuais
        # registros de meses vizinhos que a fonte às vezes inclui por engano
        .filter((F.col("trip_year") == 2023) & (F.col("trip_month").between(1, 5)))
    )

    rows_after = silver_df.count()
    print(f"Registros antes da limpeza: {rows_before:,} | depois: {rows_after:,} "
          f"({rows_before - rows_after:,} removidos)")

    # 5) Grava a tabela de consumo (silver), particionada por ano/mês da corrida
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")

    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .partitionBy("trip_year", "trip_month")
        .option("overwriteSchema", "true")
        .option("path", SILVER_PATH)
        .saveAsTable(SILVER_TABLE)
    )

    print(f"Tabela silver criada/atualizada: {SILVER_TABLE} ({SILVER_PATH})")
    silver_df.printSchema()
    spark.sql(f"SELECT trip_year, trip_month, COUNT(*) AS qtd FROM {SILVER_TABLE} GROUP BY 1,2 ORDER BY 1,2").show()

    return silver_df

# COMMAND ----------

# DBTITLE 1,Cell 5
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    bronze_df = spark.table(BRONZE_TABLE)
    print(f"Total de registros na bronze: {bronze_df.count():,}")

    silver_df = bronze_df.select(*REQUIRED_COLUMNS)

    silver_df = (
        silver_df.withColumn("VendorID", F.col("VendorID").cast("int"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
    )

    silver_df = (
        silver_df.withColumn("trip_year", F.year("tpep_pickup_datetime"))
        .withColumn("trip_month", F.month("tpep_pickup_datetime"))
        .withColumn("trip_hour", F.hour("tpep_pickup_datetime"))
    )

    rows_before = silver_df.count()

    silver_df = (
        silver_df
        .dropDuplicates()
        .filter(F.col("passenger_count").isNotNull() & (F.col("passenger_count") > 0))
        .filter(F.col("total_amount").isNotNull() & (F.col("total_amount") >= 0))
        .filter(
            F.col("tpep_pickup_datetime").isNotNull()
            & F.col("tpep_dropoff_datetime").isNotNull()
            & (F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))
        )
        .filter((F.col("trip_year") == 2023) & (F.col("trip_month").between(1, 5)))
    )

    rows_after = silver_df.count()
    print(f"Registros antes da limpeza: {rows_before:,} | depois: {rows_after:,} "
          f"({rows_before - rows_after:,} removidos)")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")

    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .partitionBy("trip_year", "trip_month")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_TABLE)
    )

    print(f"Tabela silver criada/atualizada: {SILVER_TABLE}")
    silver_df.printSchema()
    spark.sql(f"SELECT trip_year, trip_month, COUNT(*) AS qtd FROM {SILVER_TABLE} GROUP BY 1,2 ORDER BY 1,2").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.yellow_tripdata LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     trip_year,
# MAGIC     trip_month,
# MAGIC     ROUND(AVG(total_amount), 2) AS avg_total_amount
# MAGIC FROM silver.yellow_tripdata
# MAGIC GROUP BY trip_year, trip_month
# MAGIC ORDER BY trip_year, trip_month;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     trip_hour,
# MAGIC     ROUND(AVG(passenger_count), 2) AS avg_passenger_count
# MAGIC FROM silver.yellow_tripdata
# MAGIC WHERE trip_year = 2023
# MAGIC   AND trip_month = 5
# MAGIC GROUP BY trip_hour
# MAGIC ORDER BY trip_hour;
