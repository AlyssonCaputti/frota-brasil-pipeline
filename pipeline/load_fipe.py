"""Carrega o catalogo FIPE achatado em raw.fipe_versoes.

Uso:
    python -m pipeline.load_fipe
"""
import csv
import io
import sys

import psycopg2

from pipeline import config, fipe_client


def main():
    catalogo = fipe_client.carregar_catalogo()

    buf = io.StringIO()
    w = csv.writer(buf, delimiter="\t")
    n = 0
    for linha in fipe_client.iter_versoes(catalogo):
        w.writerow(linha)
        n += 1
    buf.seek(0)

    conn = psycopg2.connect(config.pg_dsn())
    try:
        with conn.cursor() as cur:
            cur.execute("create schema if not exists raw")
            cur.execute("truncate table raw.fipe_versoes")
            cur.copy_expert(
                "COPY raw.fipe_versoes "
                "(marca_codigo, marca_nome, modelo_codigo, modelo_nome) "
                "FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')",
                buf,
            )
        conn.commit()
    finally:
        conn.close()

    print(f"ok: {n:,} versoes FIPE em raw.fipe_versoes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
