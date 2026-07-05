# frota-brasil-pipeline

Data pipeline that joins Brazil's registered vehicle fleet (SENATRAN) with
technical specs from FIPE, to answer, per **brand / model / year**:

- how many vehicles are on the road (fleet count, down to municipality level);
- engine, fuel, power and (partially) wheel/tyre size.

Scope: passenger cars + light commercials only. All sources are free
(SENATRAN open data + public FIPE API). No paid data.

## Architecture

```
SENATRAN dump (CKAN) ──┐
                       ├─> raw (postgres) ─┬─> stg_frota ─┐
FIPE API ──────────────┘                   └─> stg_fipe ──┤
                                                          ├─> int_frota_carros ─> mart_frota_municipio
                                            int_fipe_specs┘                    └─> mart_consolidada
```

| Layer | Tech | What it does |
|---|---|---|
| Extraction | Python | Resolve + download the SENATRAN dump (CKAN API), pull the FIPE catalog, load both into the `raw` schema (`COPY`, batched) |
| Storage | PostgreSQL 16 | Landing (`raw`) + dbt-built schemas, via Docker Compose |
| Transform | dbt (postgres) | `staging -> intermediate -> marts`, normalization as macros, brand de-para as a seed |
| Tests | dbt tests | `not_null` / `unique` / `accepted_range` + a custom fleet-weighted coverage test |
| Orchestration | Airflow | Monthly DAG: download -> load -> dbt run -> dbt test |

## Quickstart (sample data, no 1.2 GB download)

```bash
cp .env.example .env            # USE_SAMPLE=1 is the default

python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

docker compose up -d            # postgres on :5432

python -m pipeline.explore_senatran   # sanity-check the sample
python -m pipeline.load_frota         # -> raw.frota_municipio
python -m pipeline.load_fipe          # -> raw.fipe_versoes

cd dbt
export DBT_PROFILES_DIR=$PWD
dbt deps && dbt seed && dbt run && dbt test
```

Query the result:

```sql
select marca, modelo_base, ano, qtd_veiculos, combustiveis, potencia_cv_mediana
from analytics_marts.mart_consolidada
order by qtd_veiculos desc
limit 20;
```

To run against the **full** dataset instead of the sample: set `USE_SAMPLE=0`
in `.env` and run `python -m pipeline.download_senatran` first (~128 MB zip,
~1.14 GB extracted, ~22M rows).

## Results (full April/2026 dataset)

| Metric | Value |
|---|---|
| Total vehicles in the raw dump | 133.8M |
| Classified as car / light commercial | 77.2M |
| Coverage vs. official light-vehicle total (82.9M) | **93.1%** |
| Fuel / engine spec coverage (fleet-weighted) | 86.2% |
| Power (cv) coverage | 48.0% (only when FIPE version name carries "Ncv") |
| Wheel / tyre coverage | 81.9% (optional enrichment step) |

The ~6.9% coverage gap is brands/models whose SENATRAN spelling didn't match
FIPE — candidates for extending the de-para seed.

> On the small committed sample the car share looks lower (~30%) because a
> single sampled row weighs the same whether it's a mass-market car or a
> one-off motorcycle; the 93.1% is on the full, volume-weighted fleet.

## Known limitations

- **Power at 48%** — FIPE only exposes power inside the free-text version name,
  and only for some versions. A better source would be needed to close this.
- **Wheel/tyre is partial and not in the core dbt flow** — the only free source
  has anti-bot and doesn't catalog pre-1990 classics (Fusca, Kombi, Opala...).
  Kept as an optional step, not a hard dependency of the marts.
- **`model_base` is a heuristic** — first canonical token of the model name.
  It works for the vast majority but merges some distinct trims; good enough for
  fleet-level aggregation, not for VIN-level precision.

See [docs/sources.md](docs/sources.md) and [docs/decisions.md](docs/decisions.md).

## License

MIT — see [LICENSE](LICENSE). SENATRAN and FIPE data belong to their
respective sources; this repo only ships a small sample for demonstration.
