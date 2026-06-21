# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão - Landing Zone -> Bronze (Delta Lake)
# MAGIC
# MAGIC Lê todos os arquivos `.parquet` da landing zone com **PySpark** e grava
# MAGIC uma tabela Delta `bronze.yellow_tripdata`, mantendo o schema original
# MAGIC dos dados (sem regra de negócio), apenas adicionando colunas de
# MAGIC controle de ingestão e particionamento técnico.

# COMMAND ----------

import os
import sys
from pyspark.sql import SparkSession, functions as F

PROJECT_ROOT = "/Workspace/Repos/ifood/ifood-01/ifood-case"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.utils.config import (
    IS_DATABRICKS,
    LANDING_PATH,
    BRONZE_PATH,
    BRONZE_DB,
    BRONZE_TABLE,
    YEAR,
    MONTHS,
)

# COMMAND ----------

def get_spark() -> SparkSession:
    if IS_DATABRICKS:
        return SparkSession.builder.getOrCreate()
    # Sessão local com suporte a Delta Lake
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("ifood-case-landing-to-bronze")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, DoubleType, StringType, TimestampNTZType
)

def normalize_yellow_schema(df):
    # Corrige diferença airport_fee vs Airport_fee
    if "Airport_fee" in df.columns and "airport_fee" not in df.columns:
        df = df.withColumnRenamed("Airport_fee", "airport_fee")

    return (
        df
        .withColumn("VendorID", F.col("VendorID").cast("long"))
        .withColumn("passenger_count", F.col("passenger_count").cast("double"))
        .withColumn("RatecodeID", F.col("RatecodeID").cast("double"))
        .withColumn("PULocationID", F.col("PULocationID").cast("long"))
        .withColumn("DOLocationID", F.col("DOLocationID").cast("long"))
        .withColumn("payment_type", F.col("payment_type").cast("long"))
        .withColumn("airport_fee", F.col("airport_fee").cast("double"))
    )

def run():
    spark = SparkSession.builder.getOrCreate()

    dfs = []

    for month in MONTHS:
        month_path = f"{LANDING_PATH}/year={YEAR}/month={month:02d}/*.parquet"
        print(f"Lendo mês {month:02d}: {month_path}")

        df_month = spark.read.parquet(month_path)

        df_month = (
            normalize_yellow_schema(df_month)
            .withColumn("_source_file", 
                        F.col("_metadata.file_path"))
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_year", F.lit(YEAR))
            .withColumn("_month", F.lit(month))
        )

        dfs.append(df_month)

    df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)

    print(f"Total de registros lidos da landing zone: {df.count():,}")
    df.printSchema()

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("_year", "_month")
        .option("overwriteSchema", "true")
        .saveAsTable(BRONZE_TABLE)
    )

    print(f"Tabela bronze criada/atualizada: {BRONZE_TABLE} ({BRONZE_PATH})")

    spark.sql(f"""
        SELECT _year, _month, COUNT(*) AS qtd
        FROM {BRONZE_TABLE}
        GROUP BY 1,2
        ORDER BY 1,2
    """).show()

    return df

# COMMAND ----------

if __name__ == "__main__":
    run()
