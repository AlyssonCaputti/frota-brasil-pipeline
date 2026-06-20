"""Carrega o dump de frota (TXT ou amostra) na tabela raw.frota_municipio.

Le em streaming e usa COPY do postgres (via copy_expert) por batches -
o arquivo full tem 22M linhas, nao da pra segurar em memoria nem fazer
INSERT linha a linha.

Uso:
    python -m pipeline.load_frota
"""
import csv
import io
import sys

import psycopg2

from pipeline import config

BATCH = 100_000


def caminho_fonte():
    if config.USE_SAMPLE:
        return config.SAMPLES_DIR / "senatran_frota_sample.csv"
    return config.RAW_DIR / (
        f"i_frota_por_uf_municipio_marca_e_modelo_ano_{config.SENATRAN_MES}.TXT"
    )


def aplicar_ddl(cur):
    ddl = (config.ROOT / "pipeline" / "sql" / "raw_ddl.sql").read_text(encoding="utf-8")
    cur.execute(ddl)


def copiar_batch(cur, linhas):
    """Manda um batch via COPY. linhas = lista de tuplas ja prontas."""
    buf = io.StringIO()
    w = csv.writer(buf, delimiter="\t")
    for t in linhas:
        w.writerow(t)
    buf.seek(0)
    cur.copy_expert(
        "COPY raw.frota_municipio "
        "(uf, municipio, marca_modelo, ano_fabricacao, qtd_veiculos, mes_referencia) "
        "FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')",
        buf,
    )


def main():
    fonte = caminho_fonte()
    if not fonte.exists():
        raise SystemExit(f"fonte nao encontrada: {fonte} "
                         f"(rodou o download? ou seta USE_SAMPLE=1)")

    print(f"carregando {fonte.name} -> raw.frota_municipio")
    conn = psycopg2.connect(config.pg_dsn())
    conn.autocommit = False
    total = 0
    try:
        with conn.cursor() as cur:
            aplicar_ddl(cur)
            with open(fonte, encoding="utf-8-sig") as f:
                reader = csv.reader(f, delimiter=";")
                next(reader)  # header
                buffer = []
                for row in reader:
                    if len(row) != 5:
                        # linha torta - loga e segue (acontece pouco mas acontece)
                        continue
                    uf, mun, mm, ano, qtd = (c.strip() for c in row)
                    buffer.append((uf, mun, mm, ano, qtd, config.SENATRAN_MES))
                    if len(buffer) >= BATCH:
                        copiar_batch(cur, buffer)
                        total += len(buffer)
                        print(f"\r  {total:,} linhas", end="", flush=True)
                        buffer = []
                if buffer:
                    copiar_batch(cur, buffer)
                    total += len(buffer)
        conn.commit()
        print(f"\nok: {total:,} linhas em raw.frota_municipio")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
