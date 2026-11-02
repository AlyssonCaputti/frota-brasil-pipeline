# Pipeline internals

## Code map

| Module | Job |
|---|---|
| `pipeline/mes.py` | `Mes`, the reference month (`abril_2026`), ordered chronologically |
| `pipeline/arquivos.py` | where each month's files live, TXT reader, malformed-row limit |
| `pipeline/download_senatran.py` | CKAN lookup, resumable download, `--ultimo` |
| `pipeline/to_parquet_spark.py` | TXT → Parquet (production); `to_parquet.py` is the no-JVM fallback |
| `pipeline/load_frota.py` / `load_fipe.py` | `COPY` into the Postgres `raw` schema |
| `pipeline/load_bq.py` | same contract, into BigQuery |
| `pipeline/backfill.py` | many years, one month in `raw` at a time |
| `pipeline/dimensiona_bq.py` | BigQuery cost per query, measured on the Parquet |

Expected failures (missing file, broken layout) raise `ErroPipeline`; the CLI
prints the message and exits 1. Anything else is a bug and keeps its traceback.

## dbt lineage

```
raw.frota_municipio ─> stg_frota ──────────────────┐
raw.fipe_versoes ───> stg_fipe ─> int_fipe_specs ──┼─> int_frota_carros ─> mart_frota_municipio ─> mart_consolidada
seed de_para_marca ─────────────────────────────────┘
```

## Normalization

SENATRAN's `Marca Modelo` is free text typed at registration:

- `VW/GOL`, `VOLKS/GOL`, `VOLKSWAGEN/GOL`: one car, three spellings → seed `de_para_marca`.
- `I/TOYOTA COROLLA`: `I/` means imported; the brand is the next token.
- `HR-V`, `T-Cross`: normalization splits them (`HR V`), `model_base` glues them back (`HRV`).
- `CITY Sedan EX 1.5 Flex` → `CITY`, the join key with FIPE.

FIPE has no engine/fuel/power fields, so `stg_fipe` parses the version name.
Fuel is matched on the unaccented name, so `(Elétrico)` and `(Híbrido)` count.

## Car filter

The dump has no vehicle type. `int_frota_carros` keeps only the
`(brand, model_base)` pairs that exist in FIPE's **car** catalog: motorcycles,
trucks and buses drop out, including motorcycles from carmakers (Honda CG out,
Honda Civic in).

## Incremental marts

Both marts are incremental by month (`unique_key = mes_referencia`). Which
months a run touches:

| Run | Months |
|---|---|
| `dbt run --vars '{meses: ["maio_2026"]}'` | exactly those (the DAG and backfill use this) |
| `dbt run` | months not yet in the mart |
| first run | all |

`mart_frota_municipio` holds the history and ignores `--full-refresh` unless
you also pass `--vars '{rebuild_historico: true}'`, because after a backfill
`raw` no longer has the old months. To re-spec history against a new FIPE
catalog, refresh only the consolidated mart:
`dbt run --select mart_consolidada --full-refresh`.

## Tests

- **Unit tests** (`models/staging/_staging.yml`): `model_base` and fuel parsing
  on fixed inputs (HR-V, T-Cross, `(Elétrico)`...). No data needed.
- **Schema tests** on the marts: not null, ranges, unique grain.
- `assert_meses_do_raw_nos_marts`: every month in `raw` reached the marts.
  Catches an empty mart and a month the incremental skipped.
- `assert_cobertura_specs`: fleet-weighted fuel coverage ≥ 70%, per month.
- `assert_de_para_marca_sem_colisao`: seed keys can't collide into different brands.
- **pytest** (`tests/`): file lookup, both converters on the same fixture,
  idempotent load with rollback, retries, BigQuery loader with a fake client.
