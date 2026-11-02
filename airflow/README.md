# Airflow

Optional: everything runs by hand too. The DAG is the monthly schedule.

## Flow

```
ultimo_mes ─> baixar ──────────┐
preparar_banco ─> carregar_frota ─┐
              └─> carregar_fipe ──┴─> dbt_run (só o mês) ─> dbt_test
```

- **When:** `FROTA_CRON`, default `0 6 15 * *` (15th, 06:00). Change it to any
  cron, e.g. `0 3 5 * *` for the 5th at 03:00.
- **Which month:** the newest one SENATRAN has published, asked from CKAN at run
  time. If it's already loaded, the run redoes it without duplicates.
- **FIPE:** downloaded again every run; if the API is down, the last cached
  catalog is used.
- `catchup=False`: older months go through `make backfill`, not the scheduler.

## Run locally

Airflow doesn't run natively on Windows; use WSL or Docker.

```bash
export AIRFLOW_HOME="$(pwd)/airflow/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/airflow/dags"
export PROJECT_ROOT="$(pwd)" PYTHONPATH="$(pwd)" USE_SAMPLE=0

pip install -r requirements-airflow.txt   # see the constraints note inside
airflow standalone
```

Open http://localhost:8080, turn on `frota_brasil` and trigger it.
