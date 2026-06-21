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

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
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
def run():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Lê todos os meses da landing zone de uma vez, com path glob,
    # já trazendo year/month como colunas a partir da estrutura de pastas.
    input_path = f"{LANDING_PATH}/year={YEAR}/month=*/*.parquet"
    print(f"Lendo landing zone em: {input_path}")

    df = (
        spark.read.option("mergeSchema", "true")
        .parquet(input_path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_year", F.lit(YEAR))
        .withColumn(
            "_month",
            F.regexp_extract(F.col("_source_file"), r"month=(\d{2})", 1).cast("int"),
        )
    )

    print(f"Total de registros lidos da landing zone: {df.count():,}")
    df.printSchema()

    # Cria o database lógico, se não existir
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

    # Grava como tabela Delta, particionada por ano/mês de ingestão.
    # overwrite + replaceWhere garante idempotência: reexecutar o pipeline
    # para o mesmo período não duplica dados.
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("_year", "_month")
        .option("overwriteSchema", "true")
        .option("path", BRONZE_PATH)
        .saveAsTable(BRONZE_TABLE)
    )

    print(f"Tabela bronze criada/atualizada: {BRONZE_TABLE} ({BRONZE_PATH})")
    spark.sql(f"SELECT _year, _month, COUNT(*) AS qtd FROM {BRONZE_TABLE} GROUP BY 1,2 ORDER BY 1,2").show()

    return df

# COMMAND ----------
if __name__ == "__main__":
    run()
