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

## Why dbt for the transforms

The normalization rules (brand de-para, model_base extraction, year validation)
were originally a pile of pandas in a `lib_normaliza.py`. Moving them into dbt
gives version-controlled SQL, lineage and testable assumptions
(`not_null`, `unique`, `relationships`, plus a custom coverage test).
