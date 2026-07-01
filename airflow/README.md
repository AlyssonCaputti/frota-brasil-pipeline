# Airflow

DAG que orquestra o pipeline mensal. Não é obrigatório pra ver o projeto
rodar — dá pra rodar tudo na mão (ver README raiz). O Airflow é a camada de
agendamento/observabilidade.

## Rodar local (standalone)

```bash
export AIRFLOW_HOME="$(pwd)/airflow/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/airflow/dags"
export PROJECT_ROOT="$(pwd)"
export PYTHONPATH="$(pwd)"      # pro import do pacote pipeline funcionar

pip install -r requirements-airflow.txt
airflow standalone              # sobe web + scheduler, cria o admin
```

Abre em http://localhost:8080, liga a DAG `frota_brasil` e dispara.

## Fluxo

```
download_senatran ─> load_frota_raw ─┐
load_fipe_raw ───────────────────────┴─> dbt_deps ─> dbt_seed ─> dbt_run ─> dbt_test
```

`schedule=@monthly`, `catchup=False` (o SENATRAN só disponibiliza o mês
corrente; não faz sentido backfillar meses que saíram do ar).
