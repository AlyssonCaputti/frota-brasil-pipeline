"""Backfill de vários anos sem estourar o disco.

31 meses no raw passam de 60 GB. Como os marts são incrementais por mês e o
staging é view, processo um mês por vez:

    carrega o mês no raw -> dbt run dos marts só daquele mês -> tira o mês do raw

O pico de disco fica em um mês, os marts guardam o histórico e o Parquet segue
como cópia fria.

    python -m pipeline.backfill --anos 2024 2025 2026
    python -m pipeline.backfill --anos 2026 --manter-raw
"""

import argparse
import json
import logging
import os
import subprocess

from psycopg2.extensions import connection

from pipeline import arquivos, cli, config, db, load_fipe
from pipeline.erros import ErroPipeline
from pipeline.load_frota import TABELA, carregar_mes

log = logging.getLogger(__name__)


def dbt(*args: str) -> None:
    env = {**os.environ, "DBT_PROFILES_DIR": str(config.DBT_DIR)}
    if subprocess.run(["dbt", *args], cwd=config.DBT_DIR, env=env).returncode:
        raise ErroPipeline(f"dbt {' '.join(args)} falhou")


def tirar_do_raw(conn: connection, mes: str) -> None:
    with conn, conn.cursor() as cur:
        meses_no_raw = db.escalar(cur, f"select count(distinct mes_referencia) from {TABELA}")
        # truncate devolve o disco na hora (delete só depois do vacuum), mas só
        # posso usar se o raw tiver apenas este mês
        if meses_no_raw <= 1:
            cur.execute(f"truncate table {TABELA}")
        else:
            cur.execute(f"delete from {TABELA} where mes_referencia = %s", (mes,))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Backfill mês a mês: raw -> marts -> limpa raw")
    ap.add_argument("--anos", type=int, nargs="+", required=True)
    ap.add_argument("--manter-raw", action="store_true", help="não tira o mês do raw")
    args = ap.parse_args(argv)

    if config.USE_SAMPLE:
        raise ErroPipeline("USE_SAMPLE=1 não combina com backfill")

    meses = [m for ano in args.anos for m in arquivos.meses_em_disco(ano)]
    if not meses:
        raise ErroPipeline(f"nenhum mês de {args.anos} em {config.RAW_DIR}; rode o download antes")
    log.info("%d meses: %s", len(meses), ", ".join(map(str, meses)))

    load_fipe.main([])
    dbt("deps")
    dbt("seed")
    dbt("run", "--select", "staging", "intermediate")

    with db.conectar() as conn:
        for i, mes in enumerate(meses, start=1):
            log.info("[%d/%d] %s", i, len(meses), mes)
            carregar_mes(conn, mes, arquivos.escolher_fonte(mes))
            dbt("run", "--select", "marts", "--vars", json.dumps({"meses": [str(mes)]}))
            if not args.manter_raw:
                tirar_do_raw(conn, str(mes))

    dbt("test")
    return 0


if __name__ == "__main__":
    cli.executar(main)
