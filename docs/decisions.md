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

## Why dbt for the transforms

The normalization rules (brand de-para, model_base extraction, year validation)
were originally a pile of pandas in a `lib_normaliza.py`. Moving them into dbt
gives version-controlled SQL, lineage and testable assumptions
(`not_null`, `unique`, `relationships`, plus a custom coverage test).
