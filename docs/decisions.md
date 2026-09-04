# Scope decisions

Short log of the "why" behind the non-obvious choices. Written as I went, not
rationalized after the fact.

## Granularity

Municipality level. It's the finest grain SENATRAN publishes and it doesn't cost
anything to keep it — the marts aggregate up to national when needed.

## Vehicle scope: cars + light commercials only

No motorcycles, trucks, buses or trailers. The trick to filter them without a
vehicle-type column (the municipality dump doesn't have one) is: **keep only
`(brand, model_base)` pairs that exist in the FIPE car catalog** (a whitelist).
That drops motorcycles/trucks automatically — including the motorcycles of
brands that also make cars (Honda CG/BIZ vs Honda Civic).

## Specs coverage is partial, on purpose

- Engine/fuel come from the FIPE version name → good coverage.
- Power (cv) only when the version name carries "Ncv" → ~half.
- Wheel/tyre from a scraped source with anti-bot → partial, and the pre-1990
  classics (Fusca, Kombi, Opala...) simply aren't cataloged anywhere free.

Decision: ship what the free sources give, be explicit about the gap in the
docs, don't fake it.

## One month per load, or a whole year

`raw.frota_municipio` used to be dropped and recreated on every load, so the
table only ever held the month in `SENATRAN_MES`. The marts already carried
`mes_referencia` all the way through the lineage, so the only thing missing was
the load step: it now deletes just the month being loaded and appends it. That
makes reloading a month idempotent and lets `--ano 2026` keep 7 months side by
side. The monthly Airflow DAG stops wiping history as a side effect.

The coverage test groups by `mes_referencia` for the same reason — averaged over
a year, one broken month would hide behind the good ones.

Skipped an index on `mes_referencia`: it's a low-cardinality column and the
index would cost more during `COPY` than it saves on the once-per-month
`delete`.

## Incremental marts, partitioned by month

The marts used to be `materialized='table'`, so a full year rebuilt ~157M rows
on every `dbt run` just to add one month. Three years would be ~700M. Both
marts are now `materialized='incremental'` with `incremental_strategy='delete+insert'`
and `unique_key='mes_referencia'`.

The unique key is the **month**, not the grain (`marca+modelo_base+ano+...`).
That's deliberate: the logical partition is the whole month, which is the same
contract `load_frota.py` already honors when it deletes only the month being
loaded. Reloading a month stays idempotent end to end.

The month filter lives in one macro (`filtro_meses`) so both marts pick the
same months. `mart_consolidada` compares against **itself** rather than its
source — comparing against the source would permanently skip a month that had
landed in `mart_frota_municipio` but not yet in the consolidated table.

Two consequences worth knowing:

- FIPE specs carry no month (the catalog is always "now"), so a month already
  materialized keeps the specs that were valid when it landed. Use
  `dbt run --full-refresh` to re-spec history against a fresh catalog.
- To reprocess one corrected month without touching the rest:
  `dbt run --vars '{meses: ["maio_2026"]}'`.

## Parquet as the cold copy of raw

One month of the dump is ~1.1 GB of TXT and ~22M rows; three years would be
~35 GB of text sitting on disk. `pipeline/to_parquet.py` converts each month to
Parquet (zstd + dictionary encoding), written in row groups so peak memory
stays flat regardless of file size.

Measured on the committed 75k-row sample: **7.0x smaller** (3.98 MB → 0.57 MB).
The full dump should do better, since `municipio` and `marca_modelo` repeat far
more across 22M rows than across 75k.

Everything stays string-typed, exactly like `raw_ddl.sql` — casting here would
mean deciding the schema before looking at the data, which is precisely what
landing raw as text already avoids.

`load_frota.py` prefers the Parquet when it exists and falls back to the TXT
otherwise, so nobody who already has TXT files on disk has to convert. Parquet
is a *cold copy*, not a replacement for Postgres: it exists so a month can be
reprocessed without re-downloading 130 MB.

Deliberately **not** done: moving the warehouse to DuckDB-over-Parquet. It is
arguably the right long-term shape for 700M rows, but it swaps the engine
(`unaccent` doesn't exist in DuckDB, the loads are psycopg2, CI would change)
and nothing has been measured yet showing Postgres as the bottleneck.

## Why dbt for the transforms

The normalization rules (brand de-para, model_base extraction, year validation)
were originally a pile of pandas in a `lib_normaliza.py`. Moving them into dbt
gives version-controlled SQL, lineage and testable assumptions
(`not_null`, `unique`, `relationships`, plus a custom coverage test).
