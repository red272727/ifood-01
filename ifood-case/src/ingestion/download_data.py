# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão - Download para Landing Zone
# MAGIC
# MAGIC Baixa os arquivos `.parquet` originais de Yellow Taxi (Jan–Mai/2023)
# MAGIC diretamente da fonte oficial da NYC TLC e os grava, **sem nenhuma
# MAGIC transformação**, na landing zone do Data Lake, particionados por
# MAGIC `year=YYYY/month=MM/`.

# COMMAND ----------

import os

base = "/Workspace/Repos/ifood/ifood-01/ifood-case"

for folder in [
    "src",
    "src/utils",
    "src/ingestion",
    "src/transform",
]:
    init_file = os.path.join(base, folder, "__init__.py")
    open(init_file, "a").close()
    print("Criado:", init_file)

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -la /Workspace/Repos/ifood/ifood-01/ifood-case/src/utils

# COMMAND ----------

import sys

PROJECT_ROOT = "/Workspace/Repos/ifood/ifood-01/ifood-case"

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.config import (
    IS_DATABRICKS,
    LANDING_PATH,
    YEAR,
    MONTHS,
    source_file_name,
    source_url,
)

# COMMAND ----------

def _local_path(p: str) -> str:
    """Converte um path dbfs:/... em /dbfs/... para acesso via Python puro,
    ou mantém o path local como está fora do Databricks."""
    if p.startswith("dbfs:/"):
        return p.replace("dbfs:/", "/dbfs/")
    return p

# COMMAND ----------

def download_month(year: int, month: int) -> str:
    """Baixa o arquivo de um mês específico para a landing zone e retorna o path final."""
    url = source_url(year, month)
    file_name = source_file_name(year, month)

    target_dir = f"{LANDING_PATH}/year={year}/month={month:02d}"
    target_dir_local = _local_path(target_dir)
    os.makedirs(target_dir_local, exist_ok=True)

    target_file = f"{target_dir_local}/{file_name}"

    if os.path.exists(target_file):
        print(f"[SKIP] {file_name} já existe em {target_file}")
        return f"{target_dir}/{file_name}"

    print(f"[DOWNLOAD] {url}")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(target_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)

    print(f"[OK] Salvo em {target_file}")
    return f"{target_dir}/{file_name}"

# COMMAND ----------

def run():
    paths = []
    for month in MONTHS:
        paths.append(download_month(YEAR, month))
    print("\nArquivos disponíveis na landing zone:")
    for p in paths:
        print(f" - {p}")
    return paths

# COMMAND ----------

import os
import requests

if __name__ == "__main__":
    run()
