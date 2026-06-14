# frota-brasil-pipeline

Data pipeline that joins Brazil's registered vehicle fleet (SENATRAN) with
technical specs from FIPE, to answer, per **brand / model / year**:

- how many vehicles are on the road (fleet count, down to municipality level);
- engine, fuel, power and (partially) wheel/tyre size.

Scope: passenger cars + light commercials only. All sources are free
(SENATRAN open data + public FIPE API). No paid data.

## Stack

- **Python** — extraction (SENATRAN dump + FIPE API) into Postgres `raw` schema
- **PostgreSQL** — landing / staging (Docker Compose)
- **dbt** — transformations (`staging -> intermediate -> marts`) + data tests
- **Airflow** — orchestration (download -> load -> dbt run -> dbt test)

```
SENATRAN dump ─┐
               ├─> raw (postgres) ─> dbt staging ─> intermediate ─> marts
FIPE API ──────┘
```

## Status

Work in progress. See [docs/sources.md](docs/sources.md) for the data sources
and [docs/decisions.md](docs/decisions.md) for scope decisions.

## Sample data

`data/samples/` ships a real (but small) slice of the SENATRAN dump so the
pipeline can run end-to-end without downloading the full 1.2 GB file. Set
`USE_SAMPLE=1` in `.env`.
