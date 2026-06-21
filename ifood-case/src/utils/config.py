# Databricks notebook source
# COMMAND ----------
"""
config.py
---------
Configurações centrais do pipeline: paths das camadas do Data Lake,
período de ingestão e schema esperado dos arquivos da NYC TLC.

Detecta automaticamente se está rodando no Databricks (DBFS disponível)
ou localmente, e ajusta os paths para que o mesmo código funcione nos
dois ambientes.
"""

import os

# COMMAND ----------
# Detecta o ambiente (Databricks x local) -----------------------------------
try:
    dbutils  # noqa: F821  -> só existe dentro do Databricks
    IS_DATABRICKS = True
except NameError:
    IS_DATABRICKS = False

# COMMAND ----------
# Paths das camadas do Data Lake ---------------------------------------------
if IS_DATABRICKS:
    BASE_PATH = "dbfs:/mnt/datalake"
else:
    BASE_PATH = os.path.abspath("./datalake")

LANDING_PATH = f"{BASE_PATH}/landing/yellow_tripdata"
BRONZE_PATH = f"{BASE_PATH}/bronze/yellow_tripdata"
SILVER_PATH = f"{BASE_PATH}/silver/yellow_tripdata"

BRONZE_DB = "bronze"
SILVER_DB = "silver"
BRONZE_TABLE = f"{BRONZE_DB}.yellow_tripdata"
SILVER_TABLE = f"{SILVER_DB}.yellow_tripdata"

# COMMAND ----------
# Período do desafio: Janeiro a Maio de 2023 ---------------------------------
YEAR = 2023
MONTHS = [1, 2, 3, 4, 5]

# Fonte oficial dos arquivos (NYC TLC Trip Record Data)
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def source_file_name(year: int, month: int) -> str:
    """Nome do arquivo parquet de yellow taxi para um dado ano/mês."""
    return f"yellow_tripdata_{year}-{month:02d}.parquet"

def source_url(year: int, month: int) -> str:
    """URL pública de download do arquivo na fonte oficial NYC TLC."""
    return f"{TLC_BASE_URL}/{source_file_name(year, month)}"

# COMMAND ----------
# Colunas mínimas exigidas pelo desafio (camada de consumo / silver) --------
REQUIRED_COLUMNS = [
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]
