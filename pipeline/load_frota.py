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

# Acima desse percentual de linhas descartadas a carga aborta. Mesma ideia do
# limite que uso no sap-mysql-etl: descarte pontual acontece, descarte em massa
# e layout novo na origem se passando por linha ruim.
LIMITE_DESCARTE_PCT = 0.5


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
    descartadas = 0
    exemplos_descarte = []
    try:
        with conn.cursor() as cur:
            aplicar_ddl(cur)
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
                    buffer.append((uf, mun, mm, ano, qtd, config.SENATRAN_MES))
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
            print(f"\n  ATENCAO: {descartadas:,} de {lidas:,} linha(s) "
                  f"descartada(s) ({pct:.2f}%) por numero de campos != 5")
            for ex in exemplos_descarte:
                print(f"    {ex}")

        # Piso de 0.5%: acima disso nao e sujeira pontual da origem, e mudanca
        # de layout. Melhor abortar e ficar com o dado do mes passado do que
        # subir uma tabela com buraco que ninguem vai notar.
        if pct > LIMITE_DESCARTE_PCT:
            raise ValueError(
                f"descarte de {pct:.2f}% passou do limite de "
                f"{LIMITE_DESCARTE_PCT}% ({descartadas:,} de {lidas:,} linhas). "
                f"Abortei: isso tem cara de layout novo na origem, nao de linha ruim."
            )

        conn.commit()
        print(f"\nok: {total:,} linhas em raw.frota_municipio"
              + (f" ({descartadas:,} descartada(s))" if descartadas else ""))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
