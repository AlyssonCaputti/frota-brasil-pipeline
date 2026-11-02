"""DAG mensal: pega o mês mais recente que o SENATRAN publicou e atualiza os marts.

Roda num dia fixo (FROTA_CRON, padrão dia 15 às 06h). O SENATRAN publica com
atraso variável, então não deduzo o mês pela data: pergunto ao CKAN qual é o
último publicado. Se ele já estava carregado, a carga é refeita (idempotente)
e o dbt reprocessa só aquele mês.
"""

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/frota-brasil-pipeline")
DBT_DIR = f"{PROJECT_ROOT}/dbt"
MES = "{{ ti.xcom_pull(task_ids='ultimo_mes') }}"


def _dbt(task_id: str, comando: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {DBT_DIR} && {comando}",
        env={"DBT_PROFILES_DIR": DBT_DIR},
        append_env=True,
    )


@dag(
    schedule=os.getenv("FROTA_CRON", "0 6 15 * *"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"owner": "frota", "retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["frota", "senatran", "dbt"],
)
def frota_brasil():
    @task
    def ultimo_mes() -> str:
        from pipeline.download_senatran import ultimo_publicado

        return str(ultimo_publicado())

    @task
    def preparar_banco() -> None:
        from pipeline import db

        with db.conectar() as conn:
            db.aplicar_ddl(conn)

    @task
    def baixar(mes: str) -> None:
        from pipeline.download_senatran import main

        main(["--mes", mes, "--parquet"])

    @task
    def carregar_frota(mes: str) -> None:
        from pipeline.load_frota import main

        main(["--mes", mes])

    @task
    def carregar_fipe() -> None:
        from pipeline.load_fipe import main

        main(["--atualizar"])

    mes = ultimo_mes()
    banco = preparar_banco()
    frota = carregar_frota(mes)
    fipe = carregar_fipe()
    banco >> [frota, fipe]
    baixar(mes) >> frota

    dbt_run = _dbt("dbt_run", f"dbt deps && dbt seed && dbt run --vars '{{meses: [\"{MES}\"]}}'")
    [frota, fipe] >> dbt_run >> _dbt("dbt_test", "dbt test")


frota_brasil()
