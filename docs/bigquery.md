# BigQuery: the migration, sized before it's built

The dbt target, the model configs, the external-table DDL and the sizing that
justifies them.

The configs are not just *written*, they are **resolved by `dbt-bigquery`
itself**. `dbt parse --target bq` registers the BigQuery adapter and accepts
them, and the manifest shows what each target actually gets — same models, same
code:

```
$ dbt parse --target bq && <read target/manifest.json>
target usado: bigquery
  mart_consolidada       strategy=insert_overwrite  partition={'field': 'mes_data',
                         'data_type': 'date', 'granularity': 'month'}  cluster=['marca', 'modelo_base']
  mart_frota_municipio   strategy=insert_overwrite  partition=... (idem)

$ dbt parse            && <read target/manifest.json>
target usado: postgres
  mart_consolidada       strategy=delete+insert     partition=None  cluster=None
  mart_frota_municipio   strategy=delete+insert     partition=None  cluster=None
```

The partitioning and clustering are valid for BigQuery and provably inert on
Postgres — same models, same code, different resolved config per target.

That was true for the configs; it wasn't true for the SQL underneath them.
Running the staging layer against a real BigQuery Sandbox project (free, no
billing) failed on the first two models: `unaccent()`, `substring(x from
position(...))`, raw `split_part`, `!~`/`~*`, and `percentile_cont(...) within
group (...)` are all Postgres-only. Fixed with a `target.type == 'bigquery'`
branch per divergence (`_sem_acento`, `_regexp_extract`, `_mediana`, etc. — see
`docs/decisions.md`). The run then found a second, unrelated bug: `de_para_marca`
had two normalized-key collisions causing row fan-out in `stg_frota`, latent on
Postgres too, invisible in the headline numbers only because the affected
brand (Mercedes-Benz) doesn't survive the FIPE whitelist. Both fixed and
verified — `dbt test` passes 12/12 on Postgres, and `stg_frota`/`stg_fipe`/
`int_fipe_specs`/`int_frota_carros` all built on real BigQuery with row counts
matching Postgres exactly (22,423,952 / 7,366 / 991 / 13,024,622).

What didn't get proven end-to-end: the final partitioned marts, against our
April/2026 test data. Not a bug — BigQuery Sandbox enforces a 60-day partition
expiration that can't be overridden (`OPTIONS(partition_expiration_days=null)`
does nothing), and April/2026 is well past that window relative to today. A
synthetic single-row repro with a current-day partition value confirms the
exact same `CREATE TABLE ... PARTITION BY ... CLUSTER BY ... AS` populates
correctly — the DDL is right, Sandbox just won't retain historical partitions.

## Why partition, and why by month

BigQuery on-demand bills **logical bytes of the columns a query reads**, inside
the partitions the `WHERE` leaves standing. `pipeline/dimensiona_bq.py` measures
that from the real Parquet instead of guessing:

```
$ python -m pipeline.dimensiona_bq
medido em frota_municipio_abril_2026.parquet: 22,423,952 linhas
  66 MB em disco (zstd) -> 1264 MB logicos, que e o que o on-demand cobra
historico simulado: 36 meses, 45.5 GB logicos, 807M linhas
```

That 66 MB → 1,264 MB gap is the first trap: the compressed size on disk is 19x
smaller than what BigQuery charges for. Sizing this from the Parquet file size
would have understated the bill by nearly twenty times. Parquet's own
`total_uncompressed_size` is no better — with dictionary encoding it measures
dictionary indices, not the expanded strings. The script sums actual UTF-8
lengths, which is how BigQuery counts a `STRING` (2 bytes + length).

Per-query, measured:

| Query | No partition | Month partition | Saved |
|---|---|---|---|
| National fleet by brand, 1 month | 20.3 GB | 0.6 GB | 97% |
| Municipality ranking, 1 month | 30.7 GB | 0.9 GB | 97% |
| One brand across 3 years | 20.3 GB | 20.3 GB | — |
| `select *` | 45.5 GB | 45.5 GB | — |

Month is the partition key because it's already the unit everything else uses:
`load_frota.py` deletes and reloads exactly one `mes_referencia`, and both marts
are incremental with the month as `unique_key`. On BigQuery that same contract
becomes `incremental_strategy='insert_overwrite'`, which replaces whole
partitions — the write path and the read path end up agreeing for free.

## Why cluster, and why by brand

The table above shows the case partitioning does nothing for: one brand across
the whole history. Clustering by `marca` prunes blocks instead of partitions,
and what survives is roughly the brand's share of the table — measured:

| Brand | Share of rows |
|---|---|
| VW | 14.68% |
| I (imported prefix) | 12.19% |
| HONDA | 10.82% |
| FIAT | 10.53% |
| GM | 7.70% |

1,475 distinct raw brand spellings, and the median one is well under 0.01% — so
for anything outside the top handful, clustering prunes almost everything. On
the worst case, the largest brand:

```
serie de 1 marca em 36 meses (onde a particao nao ajuda):
  so particionado:          20.3 GB  US$ 0.1152
  + cluster (marca top):     3.0 GB  US$ 0.0169
```

`modelo_base` is the second cluster key because every mart query that filters a
brand tends to filter a model next, and cluster keys only prune left to right.

## Does the money actually justify it?

Honestly: not on a single query. A full scan of three years is 26 cents.

```
custo on-demand a US$ 6.25/TiB:
  varredura completa:              US$ 0.26
  1 mes, 2 colunas (particionado): US$ 0.0032
  mesma consulta sem particao:     US$ 0.1152
```

What justifies it is frequency. The same query behind a dashboard:

```
a mesma consulta num painel, 1.000x/dia:
  particionado:  US$    3.20/dia  US$    96.02/mes
  sem particao:  US$  115.22/dia  US$  3456.74/mes
```

US$ 96/month against US$ 3,457/month, for two lines of model config. That's the
argument — not the per-query cents.

## Caveats on these numbers

- **Parquet is a proxy, not an equivalence.** BigQuery stores in Capacitor and
  applies its own encoding; the ratios between scenarios hold, the absolute
  bytes will drift.
- On-demand also bills a **10 MB minimum per table referenced**, which swallows
  the smallest queries here.
- US$ 6.25/TiB is the on-demand rate and varies by region;
  `southamerica-east1` is dearer than `us-central1`, so treat it as a floor.
- One month is measured (April/2026); the 36-month history is that month
  extrapolated. Older months are slightly smaller — the fleet grows.
- Sizing is on the **raw** table, the largest one. The marts are aggregated and
  therefore cheaper; raw is the conservative bound.

## What's in the repo

| File | What |
|---|---|
| `dbt/profiles.yml` | `bq` target, oauth, `GCP_PROJECT`/`BQ_DATASET` from env |
| `dbt/models/marts/*.sql` | `partition_by` + `cluster_by` behind `target.type == 'bigquery'`, so Postgres is untouched |
| `dbt/macros/mes_para_data.sql` | derives `mes_data` (DATE) from the text `mes_referencia`, since BigQuery won't partition on a string |
| `pipeline/sql/bq_external_frota.sql` | external table over the GCS Parquet, hive-partitioned, with `require_hive_partition_filter` |
| `pipeline/dimensiona_bq.py` | everything above, recomputed from the real data |
| `requirements-bigquery.txt` | `dbt-bigquery`, kept out of the default install |

