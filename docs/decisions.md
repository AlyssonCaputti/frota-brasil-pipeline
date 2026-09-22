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

The committed 75k-row sample gave 7.0x (3.98 MB → 0.57 MB), and the hunch that
the full dump would do better was right by a wide margin: across all seven real
months of 2026 it is **18.0x** (7.80 GB → 0.43 GB), consistently 18.0–18.1x
month to month. `municipio` and `marca_modelo` repeat far more across 22M rows
than across 75k, so the dictionary pays for itself several times over. Verified
by round-trip against the TXT: same 22,126,367 rows and same fleet total of
132,424,106 on both sides.

So three years is ~1.9 GB of Parquet, not the ~5 GB the sample ratio suggested.

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

## A PySpark path for the same conversion, and why production stays as-is

`pipeline/to_parquet_spark.py` is an alternative TXT→Parquet converter, not a
replacement — it exists to measure Spark against the pipeline above, not
because ~700M rows (~1.9 GB of Parquet) needs it. That fits in memory on one
machine, which is exactly what `to_parquet.py` already does in constant
memory. Running it in `local[*]` mode is honest about that: there's no
cluster here, and it would be dishonest to imply otherwise.

Measured on April/2026 (22,423,952 rows), same machine, same file:

| | Time | Notes |
|---|---|---|
| `to_parquet.py` (pandas/pyarrow, single-threaded `csv.reader`) | 105.3s | one Python thread parsing row by row |
| `to_parquet_spark.py` (`local[*]`, 28 cores) | 23.2–30.4s | parallel CSV parse + Parquet write |

Spark came out **3.5–4.5x faster** — the opposite of what I expected going in.
Not because "Spark is faster": because the bottleneck in the current path is
CPU-bound, single-threaded, pure-Python CSV parsing, and this machine has 28
cores Spark can split the file across. On a 4-core machine the JVM startup
cost would eat most of that advantage; the gain is a property of this
hardware and this specific bottleneck, not a general Spark-vs-Python verdict.

Round-trip verified: both Parquets have the same 22,423,952 rows, the same
`133,852,588` fleet total (`qtd_veiculos` summed after trimming — Arrow's
cast doesn't tolerate the `" 2.0"` leading space the raw TXT carries, unlike
Python's `float()`), and the same five columns.

Production stays on `to_parquet.py`: no JVM dependency, no `HADOOP_HOME`/
`winutils.exe` dance to get Parquet writes working on Windows (needed here —
Spark's Hadoop compatibility layer fails on temp-dir creation without it), and
the current path is already proven at the real backfill volumes. The
speed-up is real and worth revisiting if CSV-parsing throughput ever becomes
the actual bottleneck; it isn't yet.

## Resuming downloads instead of restarting them

A failed download used to start the file over, on the assumption that Range
wasn't reliable here. It is: the portal sends `accept-ranges: bytes` and answers
`206` with a correct `content-range` to a ranged GET.

That assumption cost a whole backfill. The ~130 MB months of 2026 survive a
restart; the ~265 MB months of 2018–2019 don't, because the connection drops
every ~100 MB and a restart never gets further than the previous attempt —
January/2019 burned every retry without completing once. With resume it
finished through three consecutive drops (56 → 111 → 168 → 266 MB).

The first attempt still starts clean, since a partial left over from an earlier
run could belong to a different version of the file. A corrupted concatenation
would be caught by the zip CRC anyway, but not starting one is cheaper.

## How far back the backfill goes: 2020

Probing the tail of each yearly zip (the central directory sits at the end, so a
ranged GET of the last 64 KB lists the contents without downloading 250 MB):

| Years | Inside the zip | Readable |
|---|---|---|
| 2013–2014 | `.mdb` | no |
| 2015–2017 | `.rar` archives | no |
| 2018–2019 | `.accdb` (MS Access) | no |
| 2020–2026 | `.TXT` | yes |

Finding this out the slow way produced a 1.95 GB `.accdb` from January/2019, so
`descompactar` now refuses a zip with no TXT in it and says why, instead of
letting the failure surface as "fonte nao encontrada" several steps later.
Reading the Access years would need `mdbtools` or `access-parser` — a new
extractor, not a tweak.

## The Access years (2018–2019): attempted, not shipped

The portal changed container, not content: `.TXT` from 2020, `.accdb` in
2018–2019, `.mdb` in 2013–2014. An extractor writing a TXT in the modern shape
would let `to_parquet`, `load_frota` and the whole dbt lineage stay ignorant of
where the month came from. I tried to build it against January/2019 — a 2.05 GB
`.accdb` — and stopped. Notes, so the next attempt starts further along:

**`access-parser` (pure Python) is not viable at this file size.** Asking it for
`Layout I`, a *metadata* table with six columns, consumed **5.8 GB of RAM and
646 s of CPU without finishing**. It appears to materialize the whole database
regardless of the table requested, and `parse_table` has no streaming API — it
returns `{column: [values]}` for everything.

**`pyodbc` with the native ACE driver is the right shape and still blocked.** It
connects in 7.3 s and reads schema cheaply, so memory and speed stop being the
problem. What blocks it is the file itself: the data lives in
`f_D1526399FEE243B19F3FB88B6E1000A0_Data`, a GUID-named object that ODBC does
not return from `tables()` at all, and follow-up queries hang. Only `Layout I`
is exposed as a real table. Whatever produced these files didn't lay them out
like an ordinary Access database.

So the backfill floor stays **2020**, and it isn't a line of code away. The
honest next step is `mdb-export` from mdbtools on Linux, which reads the file
format directly rather than through the Access engine — untested here, since
mdbtools has no Windows build and this machine has no Linux.

## Making the staging layer actually cross-dialect

`norm_txt`/`model_base` compiled fine on Postgres and were assumed portable
because the BigQuery target parsed and resolved partition/cluster configs
correctly. Parsing isn't running: the first real `dbt run --target bq`
failed on the very first two models, because the staging layer used a pile
of Postgres-only syntax that dbt's Jinja layer never checks:

- `unaccent()` — a Postgres extension function, no BigQuery equivalent at all.
- `regexp_replace(str, pattern, repl, 'g')` — the 4th "global" flag arg;
  BigQuery's `REGEXP_REPLACE` has no flags and already replaces every match.
- `substring(x from position(y in x) + 1)` — ANSI syntax BigQuery doesn't
  parse. `substr()`/`strpos()` are functions, not syntax, and exist with the
  same signature on both engines — swapping to those needed no new macro.
- `split_part()` raw — Postgres-only; `dbt.split_part()` is the cross-dialect
  version already used by `mes_para_data`.
- `!~` / `~` / `~*` (regex non-match, match, case-insensitive match) — none
  exist on BigQuery, which uses `REGEXP_CONTAINS(str, r'...')`, with `(?i)`
  prefixed onto the pattern for case-insensitivity.
- `(regexp_match(str, pattern))[1]` — Postgres returns an array you index;
  BigQuery's `REGEXP_EXTRACT` returns the captured group directly.
- `percentile_cont(0.5) within group (order by x)` — an ordered-set aggregate
  syntax BigQuery doesn't recognize as an aggregate at all (there it's an
  analytic/window function, incompatible with a plain `GROUP BY`).
  `APPROX_QUANTILES(x, 2)[OFFSET(1)]` is the closest aggregate equivalent —
  approximate rather than exact, acceptable here since it only guards against
  one outlier trim pulling the median.

Each divergence got a `target.type == 'bigquery'` branch in a macro
(`_sem_acento`, `_regexp_replace_g`, `_regexp_match`, `_regexp_match_i`,
`_regexp_extract`, `_split_part`, `_depois_do_primeiro`, `_depois_do_ultimo`,
`_mediana`), so the Postgres branch is untouched byte-for-byte and the
BigQuery branch is what actually ran and got verified against real data.

## A real bug the BigQuery run surfaced: de-para fan-out

Getting the staging layer to compile was necessary but not sufficient —
`stg_frota` still produced 23,233,938 rows against a `raw.frota_municipio`
of exactly 22,423,952. The extra ~810k rows were a genuine, pre-existing
duplication bug, present in Postgres too (not something the BigQuery port
introduced): `de_para_marca` has two collisions after normalization —
`M.BENZ`/`M BENZ` and `MERCEDES BENZ`/`MERCEDES-BENZ` both collapse to the
same key, so the `left join` in `stg_frota` fanned out, attaching the same
`marca_padrao` to a frota row twice.

It never showed up in the headline numbers because Mercedes-Benz doesn't
survive the FIPE whitelist join in `int_frota_carros` — the duplicate rows
got filtered out before they could inflate `mart_frota_municipio`'s totals.
That's luck, not correctness: the day a colliding brand *does* survive the
whitelist, its fleet count silently doubles.

Fixed at the root: `stg_frota` now joins against a `select distinct
(normalized_key, marca_padrao)` view of the seed instead of the raw seed
with an inline `norm_txt()` on the join condition. That collapses the two
known collisions (they map to the same `marca_padrao`) without picking a
side arbitrarily. `assert_de_para_marca_sem_colisao` is the safety net for
the worse case — a normalized key mapping to *different* `marca_padrao`
values, where `distinct` wouldn't be enough and the fan-out would come back.

## Running dbt against a real BigQuery project (Sandbox, no billing)

Cheap and honest way to test the BigQuery target beyond `dbt parse`: BigQuery
Sandbox gives a real GCP project with no credit card, ~1 TB query processing
and 10 GB storage free, permanently. `dbt run --target bq` against it, with
`raw.frota_municipio` and `raw.fipe_versoes` loaded from the same April/2026
data already validated on Postgres, is what caught both issues above.

It also surfaced a Sandbox-specific limit worth knowing, not a pipeline bug:
partitioned tables in Sandbox mode carry a hard 60-day partition expiration
(`OPTIONS(partition_expiration_days=...)` does not override it — tried,
confirmed with a 2-row synthetic repro). Our test partition value was
`2026-04-01`; against the real server date (`CURRENT_DATE()`, verified
directly), that's ~174 days old, well past the 60-day floor, so the
partitioned `CREATE TABLE ... AS SELECT` runs, scans the source rows (visible
in the job's bytes-processed stat), and writes zero rows — silently, no
error. The same DDL with a recent partition value populates normally
(confirmed with a synthetic single-row repro), so the partition/cluster
logic itself is correct; it's the historical test data that Sandbox won't
retain. Proving the marts populate against a *current* month would need
either a project with billing enabled or waiting for data inside the
60-day window — out of scope for what this repo needs to demonstrate.

## Why dbt for the transforms

The normalization rules (brand de-para, model_base extraction, year validation)
were originally a pile of pandas in a `lib_normaliza.py`. Moving them into dbt
gives version-controlled SQL, lineage and testable assumptions
(`not_null`, `unique`, `relationships`, plus a custom coverage test).
