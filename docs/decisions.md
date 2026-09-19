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

Trade-off I'm accepting for now: the marts are still `materialized='table'`, so
a full year rebuilds ~157M rows on every `dbt run`. Making them incremental on
`mes_referencia` is the obvious next step, not done yet. Also skipped an index
on `mes_referencia`: it's a 7-value column and the index would cost more during
`COPY` than it saves on the once-per-month `delete`.

## Parquet as the cheap source layer

The monthly TXT is ~1.2 GB and has to be re-read in full every time. The same
month as Parquet with zstd is **18x smaller** (1.20 GB -> 0.07 GB, measured on
April/2026, 22.4M rows), because `uf`, `municipio` and `marca_modelo` are a
handful of distinct values repeated millions of times and dictionary encoding
eats that for breakfast.

Columns stay as text, same five as `raw.frota_municipio`: the contract with the
dbt staging layer doesn't change, cleaning stays where it already is. The month
goes in the directory name (`mes_referencia=abril_2026/`), which is the Hive
layout BigQuery expects from an external table — so this doubles as the landing
zone for the warehouse migration.

Unlike the load, a malformed row here is only counted and skipped. The 0.5%
abort belongs to `load_frota`, where data becomes a table; this step is a format
conversion of the source.

## How far back the backfill actually goes: 2020

The portal changed the dump format over the years, and only the recent half is
usable here. Probing the tail of each yearly zip (the central directory is at
the end, so a ranged GET of the last 64 KB reveals the filenames without
downloading 250 MB):

| Years | Inside the zip | Readable |
|---|---|---|
| 2013–2014 | `.mdb` | no |
| 2015–2017 | `.rar` archives | no |
| 2018–2019 | `.accdb` (MS Access) | no |
| 2020–2026 | `.TXT` | yes |

So "history since 2019" isn't a thing without an Access reader (`mdbtools` on
Linux, or `access-parser`). From 2020 it's 79 months, ~9.2 GB zipped. Downloading
January/2019 to find this out produced a 1.95 GB `.accdb` — hence `descompactar`
now says so explicitly instead of letting the load fail with a confusing
"fonte nao encontrada".

## Resuming downloads instead of restarting them

First version retried a failed download by starting the file over. That works
for the ~130 MB months of 2026 and fails for the ~265 MB months of 2019: the
portal drops the connection every ~100 MB, so a full restart never converges —
it burned all 4 attempts without finishing once.

The server does send `accept-ranges: bytes` and answers `206` to a ranged GET,
so retries now resume from the bytes already on disk. First attempt still starts
clean, since a partial left over from an older run could belong to a different
version of the file.

## Why dbt for the transforms

The normalization rules (brand de-para, model_base extraction, year validation)
were originally a pile of pandas in a `lib_normaliza.py`. Moving them into dbt
gives version-controlled SQL, lineage and testable assumptions
(`not_null`, `unique`, `relationships`, plus a custom coverage test).
