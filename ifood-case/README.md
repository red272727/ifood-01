# iFood Case - Data Architect | NYC Taxi Trip Data

Solução de ingestão, modelagem e disponibilização dos dados de corridas de
táxi de NY (Yellow Taxi), referentes a **Janeiro–Maio/2023**, usando PySpark
no Databricks Community Edition.

## Arquitetura (Medallion / Data Lake)

```
                 ┌────────────────────┐
 NYC TLC (HTTPS) │  download_data.py  │  baixa os .parquet originais
  (.parquet)     └─────────┬──────────┘
                            ▼
   ┌───────────────────────────────────────────┐
   │  LANDING ZONE  (dbfs:/mnt/datalake/landing) │  arquivos originais,
   │  /yellow_tripdata/year=2023/month=01..05/   │  imutáveis, "as-is"
   └─────────────────────┬─────────────────────┘
                          ▼  landing_to_bronze.py (PySpark, leitura + ingestão)
   ┌───────────────────────────────────────────┐
   │  BRONZE (raw)  (dbfs:/mnt/datalake/bronze)  │  Delta Table, schema bruto
   │  tabela: bronze.yellow_tripdata             │  + colunas de controle
   └─────────────────────┬─────────────────────┘
                          ▼  bronze_to_silver.py (limpeza, tipagem, dedup)
   ┌───────────────────────────────────────────┐
   │  SILVER (consumo)  /datalake/silver         │  Delta Table particionada,
   │  tabela: silver.yellow_tripdata             │  otimizada para consulta SQL
   └─────────────────────┬─────────────────────┘
                          ▼
              Usuários finais consultam via SQL
              (Databricks SQL / Spark SQL / notebooks)
```

### Por que essa arquitetura?

- **Landing Zone**: guarda os arquivos originais (.parquet) exatamente como
  baixados da fonte (NYC TLC). Garante reprocessamento e auditoria a
  qualquer momento, sem precisar baixar os dados de novo.
- **Bronze**: primeira ingestão estruturada em formato **Delta Lake**
  (ACID, schema enforcement, time travel), mantendo todas as colunas
  originais + colunas de controle (`_ingestion_date`, `_source_file`,
  `_year`, `_month`). Não há transformação de regra de negócio aqui —
  só padronização técnica.
- **Silver (camada de consumo)**: aplica limpeza, tipagem e seleção das
  colunas exigidas pelo desafio (`VendorID`, `passenger_count`,
  `total_amount`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`), além
  de colunas derivadas úteis para análise (`trip_year`, `trip_month`,
  `trip_hour`). É particionada por `trip_year`/`trip_month` para
  performance de consulta, e é a tabela exposta aos usuários finais via
  SQL (`SELECT * FROM silver.yellow_tripdata`).
- **Delta Lake** foi escolhido como tecnologia de metadados/tabela porque:
  já é nativo no Databricks (sem custo de setup), suporta `MERGE`/upsert
  para idempotência, schema evolution, time travel para debug e é
  consultável via SQL puro pelos usuários finais — atendendo o requisito
  "disponibilizar para os usuários consumirem via SQL".

## Estrutura do Repositório

```
ifood-case/
├─ src/
│  ├─ ingestion/
│  │  ├─ download_data.py        # baixa os .parquet da fonte para a landing zone
│  │  └─ landing_to_bronze.py    # lê a landing zone e grava a tabela bronze (Delta)
│  ├─ transform/
│  │  └─ bronze_to_silver.py     # limpeza/seleção de colunas e grava a tabela silver (Delta)
│  └─ utils/
│     └─ config.py               # paths, constantes e schema esperado
├─ analysis/
│  └─ perguntas.py               # respostas das duas perguntas (PySpark + SQL)
├─ README.md
└─ requirements.txt
```

## Como executar

1. Suba um cluster no **Databricks Community Edition** (runtime com Spark
   3.x já vem com Delta Lake nativo, não precisa instalar nada extra).
2. Importe os arquivos de `src/` e `analysis/` como notebooks (ou cole o
   conteúdo de cada `.py` em um notebook — eles já têm os marcadores
   `# COMMAND ----------` usados pelo Databricks para separar células).
3. Rode em ordem:
   1. `src/ingestion/download_data.py` → baixa os arquivos Jan–Mai/2023
      direto da URL pública da NYC TLC para a landing zone (DBFS).
   2. `src/ingestion/landing_to_bronze.py` → ingere a landing zone na
      tabela Delta `bronze.yellow_tripdata`.
   3. `src/transform/bronze_to_silver.py` → gera a tabela Delta de
      consumo `silver.yellow_tripdata`, particionada por ano/mês.
   4. `analysis/perguntas.py` → responde as duas perguntas do case
      usando a tabela `silver.yellow_tripdata`.
4. Após rodar, os usuários finais podem consultar diretamente via SQL:
   ```sql
   SELECT * FROM silver.yellow_tripdata LIMIT 10;
   ```

## Execução local (fora do Databricks)

Também é possível rodar localmente com PySpark + pacote `delta-spark`
(ver `requirements.txt`). Os scripts detectam o ambiente e ajustam os
paths automaticamente (ver `src/utils/config.py`), usando uma pasta
local (`./datalake/...`) no lugar do DBFS quando não há `dbutils`
disponível.

```bash
pip install -r requirements.txt
python src/ingestion/download_data.py
python src/ingestion/landing_to_bronze.py
python src/transform/bronze_to_silver.py
python analysis/perguntas.py
```

## Perguntas respondidas (em `analysis/perguntas.py`)

1. **Qual a média de valor total (`total_amount`) recebido em um mês,
   considerando todos os yellow táxis da frota?**
2. **Qual a média de passageiros (`passenger_count`) por cada hora do
   dia que pegaram táxi no mês de maio, considerando todos os táxis da
   frota?**

Ambas as respostas são calculadas com PySpark DataFrame API e também
disponibilizadas como Spark SQL equivalente, para mostrar que a tabela
silver pode ser consumida livremente via SQL.

## Qualidade de dados / decisões de limpeza

- Linhas com `passenger_count` nulo ou ≤ 0 são descartadas da silver
  (corrida sem passageiro não é uma corrida válida para média de
  passageiros).
- Linhas com `total_amount` negativo (estornos/ajustes) são descartadas
  da silver, pois distorcem a média de receita.
- Linhas em que `tpep_dropoff_datetime < tpep_pickup_datetime` são
  descartadas (inconsistência de relógio/registro).
- Deduplicação é feita por todas as colunas-chave + datetime, para evitar
  contagem dupla em caso de reprocessamento (idempotência via
  `MERGE`/`overwrite` por partição).
- Apenas registros cujo `tpep_pickup_datetime` cai dentro do mês/ano do
  próprio arquivo são mantidos (os arquivos da TLC frequentemente trazem
  alguns poucos registros de meses vizinhos por erro de digitação).

## Possíveis evoluções (fora do escopo mínimo)

- Orquestração via Databricks Workflows / Airflow, agendada mensalmente.
- Camada Gold com agregados pré-calculados (ex.: receita média por mês,
  passageiros médios por hora) para consumo de BI sem reprocessar a
  silver inteira.
- Testes automatizados (`pytest` + `chispa`) para validar regras de
  limpeza.
- Catalogação via Unity Catalog / Hive Metastore com `COMMENT` em cada
  coluna e contrato de dados (Great Expectations).
