"""DAG mensal da frota Brasil.

Orquestra: download SENATRAN -> load frota -> load FIPE -> dbt (deps/seed/
run/test). O download e os loads sao PythonOperator chamando o pacote
pipeline; o dbt roda via BashOperator (jeito mais simples sem o astronomer
cosmos, que seria overkill pra 1 projeto).

Schedule mensal porque o SENATRAN publica o dump uma vez por mes. catchup
desligado - nao adianta reprocessar meses velhos que nao estao no portal.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# raiz do projeto (o repo montado no container). ajusta se preciso.
PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/frota-brasil-pipeline")
DBT_DIR = os.path.join(PROJECT_ROOT, "dbt")

default_args = {
    "owner": "alysson",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}


def _download(**_):
    from pipeline.download_senatran import main
    main()


def _load_frota(**_):
    from pipeline.load_frota import main
    main()


def _load_fipe(**_):
    from pipeline.load_fipe import main
    main()


with DAG(
    dag_id="frota_brasil",
    description="Consolida frota SENATRAN x specs FIPE, mensal",
    default_args=default_args,
    schedule="@monthly",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["frota", "senatran", "dbt"],
) as dag:

    download = PythonOperator(task_id="download_senatran", python_callable=_download)
    load_frota = PythonOperator(task_id="load_frota_raw", python_callable=_load_frota)
    load_fipe = PythonOperator(task_id="load_fipe_raw", python_callable=_load_fipe)

    dbt_env = {"DBT_PROFILES_DIR": DBT_DIR, **os.environ}

    # deps so precisa rodar quando muda packages.yml, mas roda rapido, deixo sempre
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps",
        env=dbt_env,
    )
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_DIR} && dbt seed",
        env=dbt_env,
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run",
        env=dbt_env,
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test",
        env=dbt_env,
    )

    # frota e fipe podem carregar em paralelo, mas os dois tem que terminar
    # antes do dbt (o dbt run precisa das duas tabelas raw).
    download >> load_frota
    [load_frota, load_fipe] >> dbt_deps >> dbt_seed >> dbt_run >> dbt_test
