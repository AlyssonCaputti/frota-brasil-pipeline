# BigQuery

Same dbt models, different target. On BigQuery the marts are partitioned by
month (`mes_data`) and clustered by `marca, modelo_base`; on Postgres those
configs resolve to nothing. The shared config lives in
`macros/config_mart_mensal.sql`.

## Running it

```bash
pip install -r requirements-bigquery.txt
gcloud auth application-default login
export GCP_PROJECT=my-project

python -m pipeline.load_bq --mes abril_2026     # Parquet -> raw.frota_municipio
cd dbt && dbt seed --target bq && dbt run --target bq && dbt test --target bq
```

`load_bq` follows the Postgres contract: everything as text, delete the month,
append it again in load jobs (free). On the Sandbox the `DELETE` fails, so a
month can be loaded once but not reloaded.

CI runs `dbt parse --target bq`, so configs that BigQuery rejects break the build.

## What a real run showed

On a Sandbox project with April/2026: the views match Postgres row for row
(22,423,952 / 7,366 / 991 / 13,024,622). The partitioned marts build empty,
because the Sandbox drops partitions older than 60 days. With a recent
partition date the same DDL fills normally. `assert_meses_do_raw_nos_marts`
fails in that situation, which is the honest outcome.

## Why partition by month

On-demand bills the logical bytes of the columns read, inside the partitions
the `WHERE` keeps. `python -m pipeline.dimensiona_bq` measures it on the real
Parquet:

```
frota_municipio_abril_2026.parquet: 22,423,952 linhas
  66 MB em disco -> 1264 MB lógicos
  histórico de 36 meses: 45.5 GB lógicos
```

The 19x gap between disk and billed bytes is the trap: sizing from file size
would understate the bill twentyfold.

| Query (36 months of history) | No partition | Month partition |
|---|---|---|
| National fleet by brand, 1 month | 20.3 GB | 0.6 GB (−97%) |
| Municipality ranking, 1 month | 30.7 GB | 0.9 GB (−97%) |
| One brand across 3 years | 20.3 GB | 20.3 GB |

Month is the key because it's already the unit of every load.

## Why cluster by brand

Partitioning does nothing for one brand across the whole history. Clustering by
`marca` prunes blocks to roughly the brand's share of rows: the largest (VW,
14.7%) goes from 20.3 GB to 3.0 GB; the median brand is under 0.01%.
`modelo_base` comes second because brand filters are usually followed by model.

## Is it worth it?

Not per query: a full scan of three years costs US$ 0.26. Behind a dashboard
running the same one-month query 1,000 times a day, it's **US$ 96/month instead
of US$ 3,457/month**, for a few lines of config.

Caveats: Parquet is a proxy for Capacitor (ratios hold, absolute bytes drift);
there's a 10 MB minimum per table; US$ 6.25/TiB is a floor
(`southamerica-east1` costs more); 36 months is one month extrapolated.
