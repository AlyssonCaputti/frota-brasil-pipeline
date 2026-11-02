"""Carrega o catálogo FIPE achatado em raw.fipe_versoes.

python -m pipeline.load_fipe              # usa o cache se existir
python -m pipeline.load_fipe --atualizar  # baixa o catálogo de novo
"""

import argparse
import csv
import io
import logging

from pipeline import cli, db, fipe_client

log = logging.getLogger(__name__)

TABELA = "raw.fipe_versoes"
COLUNAS = ("marca_codigo", "marca_nome", "modelo_codigo", "modelo_nome")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Carrega o catálogo FIPE no schema raw")
    ap.add_argument("--atualizar", action="store_true", help="ignora o cache e baixa de novo")
    args = ap.parse_args(argv)

    buf = io.StringIO()
    csv.writer(buf, delimiter="\t").writerows(
        fipe_client.iter_versoes(fipe_client.carregar_catalogo(args.atualizar))
    )
    buf.seek(0)

    with db.conectar() as conn:
        db.aplicar_ddl(conn)
        with conn, conn.cursor() as cur:
            cur.execute(f"truncate table {TABELA}")
            db.copiar(cur, TABELA, COLUNAS, buf)
            total = db.escalar(cur, f"select count(*) from {TABELA}")
    log.info("%s versões em %s", f"{total:,}", TABELA)
    return 0


if __name__ == "__main__":
    cli.executar(main)
