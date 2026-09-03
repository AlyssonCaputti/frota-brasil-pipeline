"""Carrega o dump de frota (TXT ou amostra) na tabela raw.frota_municipio.

Le em streaming e usa COPY do postgres (via copy_expert) por batches -
o arquivo full tem 22M linhas, nao da pra segurar em memoria nem fazer
INSERT linha a linha.

Cada mes e uma particao logica por mes_referencia: o load apaga so o mes
que esta entrando, entao recarregar um mes e idempotente e os outros meses
ficam onde estao.

Uso:
    python -m pipeline.load_frota                   # mes do .env
    python -m pipeline.load_frota --mes maio_2026
    python -m pipeline.load_frota --ano 2026        # todo mes ja baixado do ano
"""

import argparse
import csv
import io
import sys

import psycopg2

from pipeline import config

BATCH = 100_000

# Acima desse percentual de linhas descartadas a carga aborta. Mesma ideia do
# limite que uso no sap-mysql-etl: descarte pontual acontece, descarte em massa
# e layout novo na origem se passando por linha ruim.
LIMITE_DESCARTE_PCT = 0.5


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


def meses_do_ano(ano):
    """Meses de <ano> com TXT em data/raw, em ordem cronologica."""
    meses = [f"{m}_{ano}" for m in config.MESES]
    return [m for m in meses if config.caminho_frota(m).exists()]


def carregar_mes(cur, mes):
    """Carrega um mes e devolve (linhas carregadas, linhas descartadas)."""
    fonte = config.caminho_frota(mes)
    if not fonte.exists():
        raise SystemExit(
            f"fonte nao encontrada: {fonte} (rodou o download? ou seta USE_SAMPLE=1)"
        )

    print(f"carregando {fonte.name} -> raw.frota_municipio ({mes})")
    cur.execute("delete from raw.frota_municipio where mes_referencia = %s", (mes,))

    total = 0
    descartadas = 0
    exemplos_descarte = []
    with open(fonte, encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=";")
        next(reader)  # header
        buffer = []
        for n_linha, row in enumerate(reader, start=2):
            if len(row) != 5:
                # Linha torta. Antes isso era um `continue` calado: em 22M
                # linhas, uma mudanca de layout descartava centenas de
                # milhares e o processo ainda imprimia "ok". Agora conto,
                # guardo exemplo e aborto se passar do limite.
                descartadas += 1
                if len(exemplos_descarte) < 5:
                    exemplos_descarte.append(
                        f"linha {n_linha}: {len(row)} campo(s) -> {row[:6]}"
                    )
                continue
            uf, mun, mm, ano, qtd = (c.strip() for c in row)
            buffer.append((uf, mun, mm, ano, qtd, mes))
            if len(buffer) >= BATCH:
                copiar_batch(cur, buffer)
                total += len(buffer)
                print(f"\r  {total:,} linhas", end="", flush=True)
                buffer = []
        if buffer:
            copiar_batch(cur, buffer)
            total += len(buffer)

    lidas = total + descartadas
    pct = (descartadas / lidas * 100) if lidas else 0.0

    if descartadas:
        print(
            f"\n  ATENCAO: {descartadas:,} de {lidas:,} linha(s) "
            f"descartada(s) ({pct:.2f}%) por numero de campos != 5"
        )
        for ex in exemplos_descarte:
            print(f"    {ex}")

    # Piso de 0.5%: acima disso nao e sujeira pontual da origem, e mudanca
    # de layout. Melhor abortar e ficar com o dado do mes passado do que
    # subir uma tabela com buraco que ninguem vai notar. Avaliado por mes:
    # diluir no ano inteiro esconderia justamente o mes que quebrou.
    if pct > LIMITE_DESCARTE_PCT:
        raise ValueError(
            f"descarte de {pct:.2f}% passou do limite de "
            f"{LIMITE_DESCARTE_PCT}% ({descartadas:,} de {lidas:,} linhas) "
            f"em {mes}. Abortei: isso tem cara de layout novo na origem, "
            f"nao de linha ruim."
        )
    return total, descartadas


def main():
    ap = argparse.ArgumentParser()
    grupo = ap.add_mutually_exclusive_group()
    grupo.add_argument("--mes", action="append", help="pode repetir")
    grupo.add_argument("--ano", help="carrega todo mes do ano ja baixado")
    args = ap.parse_args()

    if args.ano:
        if config.USE_SAMPLE:
            raise SystemExit(
                "--ano nao combina com USE_SAMPLE=1: a amostra e de um mes so "
                "e seria carregada varias vezes com mes_referencia diferente."
            )
        meses = meses_do_ano(args.ano)
        if not meses:
            raise SystemExit(
                f"nenhum TXT de {args.ano} em {config.RAW_DIR}. roda o "
                f"download_senatran --ano {args.ano} primeiro."
            )
    else:
        meses = args.mes or [config.SENATRAN_MES]

    conn = psycopg2.connect(config.pg_dsn())
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            aplicar_ddl(cur)
        conn.commit()

        # uma transacao por mes: o que ja entrou fica commitado e um mes
        # torto aborta o resto sem levar os anteriores junto.
        for mes in meses:
            with conn.cursor() as cur:
                total, descartadas = carregar_mes(cur, mes)
            conn.commit()
            print(
                f"\nok: {total:,} linhas em raw.frota_municipio ({mes})"
                + (f" ({descartadas:,} descartada(s))" if descartadas else "")
            )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
