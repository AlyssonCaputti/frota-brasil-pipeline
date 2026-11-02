# frota-brasil-pipeline

![ci](https://github.com/AlyssonCaputti/frota-brasil-pipeline/actions/workflows/ci.yml/badge.svg)

I cross Brazil's registered vehicle fleet (SENATRAN) with FIPE's technical
catalog to answer, per **brand / model / year / municipality / month**: how many
cars are on the road, and with which engine, fuel and power.

Cars and light commercials only. Every source is free and public.

## How it works

```
SENATRAN (CKAN) ─> download ─> Parquet (PySpark) ─┐
FIPE API ──────────────────────────────────────────┤
                                                    ├─> raw (Postgres or BigQuery)
                                                    └─> dbt: staging ─> intermediate ─> marts
Airflow, monthly: latest published month ─> load ─> dbt run (that month) ─> dbt test
```

| Layer | What I use | Notes |
|---|---|---|
| Extract | Python + requests | Resolves the month's URL through the CKAN API, resumes broken downloads |
| Cold storage | PySpark → Parquet | 18x smaller than the TXT; aborts if more than 0.5% of rows are malformed |
| Load | `COPY` into Postgres / load jobs into BigQuery | One month per transaction, reloading is idempotent |
| Transform | dbt 1.8 | Same SQL on Postgres and BigQuery; dialect differences live in macros |
| Quality | dbt tests + unit tests, pytest, ruff, mypy | All of it runs in CI |
| Schedule | Airflow | Fixed day each month, picks the newest month SENATRAN has published |

## Run it

With the sample (75k rows, no download):

```bash
cp .env.example .env               # USE_SAMPLE=1
pip install -r requirements.txt
make pipeline                       # postgres + load + dbt run + dbt test
```

Then:

```sql
select marca, modelo_base, ano, qtd_veiculos, combustiveis, potencia_cv_mediana
from analytics_marts.mart_consolidada
order by qtd_veiculos desc
limit 20;
```

With real data, set `USE_SAMPLE=0` and:

```bash
pip install -r requirements-pyspark.txt              # needs Java 8/11/17
python -m pipeline.download_senatran --ultimo --parquet
python -m pipeline.load_frota --mes julho_2026
python -m pipeline.load_fipe --atualizar
make dbt test
```

`make help` lists everything: a full year (`make year ANO=2026`), several years
without filling the disk (`make backfill ANOS="2024 2025"`), lint and tests
(`make check`).

## Monthly schedule

The DAG runs on a fixed day (`FROTA_CRON`, default `0 6 15 * *`, the 15th at
06:00). SENATRAN publishes with a variable lag, so I don't guess the month from
the date: I ask CKAN for the newest one, download it, load it and run dbt for
that month only. If it was already loaded, everything is redone without
duplicates. Details in [airflow/README.md](airflow/README.md).

## Numbers (April/2026, full dump)

| | |
|---|---|
| Rows in the dump | 22.4M (133.8M vehicles) |
| Classified as car / light commercial | 77.1M vehicles, 93.0% of the official total |
| Fuel / engine coverage, fleet-weighted | 86.3% |
| Power coverage | 48% (FIPE only has it when the version name says "Ncv") |
| TXT → Parquet, 7 months of 2026 | 7.80 GB → 0.43 GB |

## Limits I know about

- **Power at 48%.** It only exists inside FIPE's free-text version name.
- **`modelo_base` is a heuristic**: the first canonical token of the model name.
  Good for fleet counts, not for trim-level detail.
- **2020 is the floor.** Earlier years come as Access/RAR files I don't read.
- **BigQuery Sandbox** expires partitions after 60 days and has no `DELETE`, so
  historical months only stick on a billed project.

## Docs

- [docs/pipeline.md](docs/pipeline.md): lineage, normalization rules, tests
- [docs/decisions.md](docs/decisions.md): the why behind the non-obvious choices
- [docs/bigquery.md](docs/bigquery.md): partitioning, clustering and what it costs
- [docs/sources.md](docs/sources.md): where the data comes from

MIT license. SENATRAN and FIPE data belong to their sources; the repo only ships
a small sample.
