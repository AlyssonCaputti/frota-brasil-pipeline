# Decisions

The why behind the choices that aren't obvious from the code.

## Scope

- **Municipality grain.** It's the finest SENATRAN publishes and costs nothing
  to keep; the marts roll up when needed.
- **Cars and light commercials only**, filtered by the FIPE whitelist (see
  [pipeline.md](pipeline.md)), since the dump has no vehicle type.
- **Partial specs, said out loud.** Power only exists when FIPE's version name
  has "Ncv" (~48%). I ship what free sources give and document the gap.

## One month = one partition

Every step treats a month as the unit: the load deletes and reinserts one
`mes_referencia`, the marts replace whole months, BigQuery partitions by month.
Reloading a month is idempotent end to end. No index on `mes_referencia`: low
cardinality, and it would slow `COPY` more than it helps the monthly delete.

## Raw lands as text

Everything in `raw` is a string. Casting at load time means deciding the schema
before looking at the data; dbt staging does it with validation instead.

## The monthly DAG asks for the newest month

SENATRAN publishes with a variable lag. Deriving the month from the run date
would either point at a month that isn't out yet or skip one. The DAG asks CKAN
for the newest published month and passes it explicitly to download, load and
`dbt run --vars`. A rerun redoes the same month without duplicates.

## Parquet as cold storage, PySpark as the converter

One month of TXT is ~1.1 GB; across the seven months of 2026 Parquet is
**18x smaller** (7.80 GB → 0.43 GB), because the columns repeat a lot.

PySpark is the production converter. Measured on April/2026 (22.4M rows):

| | Time |
|---|---|
| `to_parquet.py` (pyarrow, single-threaded CSV parsing) | 105s |
| Spark `local[*]` (28 cores) | 20–30s |
| Spark `local[2]` | 72s |

The win comes from parallel CSV parsing, not from Spark being magic; on two
cores the JVM startup eats most of it. Two lessons:

- Forcing one output file with `coalesce(1)` serialized the write and made Spark
  slower than the baseline (132s). I kept Spark's directory of part-files and
  made the reader (`pyarrow.dataset`) accept both layouts.
- With `DROPMALFORMED`, `df.count()` doesn't parse columns, so it never drops
  anything and reported 0 malformed rows. I now count what was actually written.

Both converters write to a temporary path and swap at the end, strip values,
and abort above 0.5% malformed rows. A broken conversion never replaces a good
Parquet, and the limit applies whether the month comes from TXT or Parquet.

`to_parquet.py` stays as the no-JVM fallback and the benchmark baseline.

## Resumable downloads

The portal accepts `Range` and answers `206`. The 265 MB months of 2018–2019
drop every ~100 MB, so restarting from zero never finished; resuming did
(56 → 111 → 168 → 266 MB). The first attempt always starts clean, since a stale
partial could belong to an older version of the file.

## 2020 is the floor

The yearly zips hold `.mdb` (2013–14), `.rar` (2015–17) and `.accdb` (2018–19).
I tried reading the 2 GB `.accdb` of January/2019: `access-parser` used 5.8 GB of
RAM without finishing, and ODBC doesn't expose the GUID-named data table. The
next attempt should be `mdb-export` from mdbtools on Linux.

## Backfill one month at a time

31 months in `raw` would be 60+ GB. `backfill.py` loads a month, runs the marts
for that month, then removes it from `raw`. Peak disk is one month; the marts
keep the history. That's why `mart_frota_municipio` refuses a plain
`--full-refresh`: rebuilding it from an emptied `raw` would wipe history.

## Cross-dialect SQL

The first real run on BigQuery failed on Postgres-only syntax: `unaccent`,
regex flags, `~`, `regexp_match(...)[1]`, `percentile_cont ... within group`,
`filter (where ...)`, `::numeric`. Each difference now lives in one macro with a
branch per dialect (`macros/norm_txt.sql`). Two fixes from the audit:

- Postgres reads `\b` as backspace, not a word boundary, so `ev\b` never matched.
  Fuel is now matched on the normalized name (`' EV '`), same on both engines.
- `model_base` turned HR-V, WR-V and ZR-V into "CRV". It now glues the tokens
  back (`HR V` → `HRV`). Both cases have unit tests.

## De-para fan-out

`de_para_marca` has keys that collide after normalization (`M.BENZ`/`M BENZ`).
The join duplicated those rows (23.2M out of 22.4M in `stg_frota`). It never
reached the totals only because Mercedes-Benz doesn't pass the FIPE whitelist.
The join now uses a `distinct` view of the seed, and a test fails if two keys
ever collide into different brands.

## BigQuery Sandbox

Real project, no billing. Two limits: partitions older than 60 days are dropped
(historical marts build empty) and there's no `DELETE`. The views match Postgres
row for row, which proves the SQL; the empty marts are a free-tier limit. The
test `assert_meses_do_raw_nos_marts` makes that visible instead of passing
trivially.

## Why dbt

The normalization used to be pandas. In dbt it's versioned SQL with lineage and
tests on the assumptions.
