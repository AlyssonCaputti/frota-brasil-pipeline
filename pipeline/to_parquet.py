"""Converte o TXT da frota SENATRAN em Parquet (zstd), um arquivo por mes.

Um mes de TXT sao ~1.1 GB; tres anos, ~35 GB. As colunas do dump sao quase
todas de baixa cardinalidade, entao dictionary + zstd derruba isso ~7x (medido
na amostra). Guardo em row groups pra nunca segurar o mes todo em memoria.

Isso e copia fria pra reprocessar um mes sem baixar de novo - nao substitui o
Postgres. O load_frota le daqui quando o arquivo existe.

Uso:
    python -m pipeline.to_parquet                  # mes do .env
    python -m pipeline.to_parquet --mes maio_2026
    python -m pipeline.to_parquet --ano 2026       # todo mes com TXT em disco
    python -m pipeline.to_parquet --ano 2026 --apagar-txt
"""

import argparse
import csv
import sys

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline import config

# 500k segura o pico de memoria em centenas de MB e ainda da row group grande
# o bastante pro zstd render.
ROW_GROUP = 500_000

# tudo string, igual o raw_ddl.sql - cast e limpeza sao do dbt.
SCHEMA = pa.schema([
    ("uf", pa.string()),
    ("municipio", pa.string()),
    ("marca_modelo", pa.string()),
    ("ano_fabricacao", pa.string()),
    ("qtd_veiculos", pa.string()),
])

COLUNAS = [c.name for c in SCHEMA]


def caminho_parquet(mes: str):
    """Parquet do mes. Fica junto do TXT, em data/raw."""
    return config.RAW_DIR / f"frota_municipio_{mes}.parquet"


def _batches(fonte):
    """Gera RecordBatch de ROW_GROUP linhas, contando linha torta."""
    descartadas = 0
    exemplos = []
    with open(fonte, encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=";")
        next(reader)  # header
        cols = [[] for _ in COLUNAS]
        n = 0
        for n_linha, row in enumerate(reader, start=2):
            if len(row) != 5:
                # mesmo criterio do load_frota: conto, nunca descarto calado
                descartadas += 1
                if len(exemplos) < 5:
                    exemplos.append(
                        f"linha {n_linha}: {len(row)} campo(s) -> {row[:6]}"
                    )
                continue
            for col, valor in zip(cols, row):
                col.append(valor.strip())
            n += 1
            if n >= ROW_GROUP:
                yield pa.record_batch(cols, schema=SCHEMA), descartadas, exemplos
                cols = [[] for _ in COLUNAS]
                n = 0
        if n:
            yield pa.record_batch(cols, schema=SCHEMA), descartadas, exemplos


def converter(mes: str, apagar_txt=False):
    """TXT -> Parquet. Devolve (linhas, descartadas, bytes_parquet)."""
    fonte = config.caminho_frota(mes)
    if not fonte.exists():
        raise SystemExit(
            f"TXT nao encontrado pro mes {mes}: {fonte} "
            f"(rodou o download_senatran?)"
        )

    destino = caminho_parquet(mes)
    destino.parent.mkdir(parents=True, exist_ok=True)
    # escrevo em .tmp e renomeio: se cair no meio, o load nao acha um parquet
    # truncado achando que esta completo.
    tmp = destino.with_suffix(".parquet.tmp")

    print(f"convertendo {fonte.name} -> {destino.name}")
    total = 0
    descartadas = 0
    exemplos = []
    writer = None
    try:
        for batch, desc, exs in _batches(fonte):
            if writer is None:
                writer = pq.ParquetWriter(
                    tmp, SCHEMA, compression="zstd", use_dictionary=True
                )
            writer.write_batch(batch)
            total += batch.num_rows
            descartadas, exemplos = desc, exs
            print(f"\r  {total:,} linhas", end="", flush=True)
    finally:
        if writer is not None:
            writer.close()
    print()

    if writer is None:
        tmp.unlink(missing_ok=True)
        raise SystemExit(f"{fonte.name} nao tem linha de dado valida, abortei")

    tmp.replace(destino)

    tam_txt = fonte.stat().st_size
    tam_pq = destino.stat().st_size
    print(
        f"  {tam_txt / 1e6:.0f} MB -> {tam_pq / 1e6:.0f} MB "
        f"({tam_txt / tam_pq:.1f}x menor)"
    )
    if descartadas:
        print(f"  ATENCAO: {descartadas:,} linha(s) com numero de campos != 5")
        for ex in exemplos:
            print(f"    {ex}")

    if apagar_txt:
        # so depois do replace, com o parquet inteiro no lugar final
        fonte.unlink()
        print(f"  apagei {fonte.name}")

    return total, descartadas, tam_pq


def meses_do_ano(ano):
    """Meses de <ano> com TXT em data/raw, em ordem cronologica."""
    meses = [f"{m}_{ano}" for m in config.MESES]
    return [m for m in meses if config.caminho_frota(m).exists()]


def main(argv=None):
    """argv explicito por causa do Airflow (ver download_senatran)."""
    ap = argparse.ArgumentParser()
    grupo = ap.add_mutually_exclusive_group()
    grupo.add_argument("--mes", action="append", help="pode repetir")
    grupo.add_argument("--ano", help="converte todo mes com TXT em disco")
    ap.add_argument(
        "--apagar-txt",
        action="store_true",
        help="apaga o TXT depois de converter (o parquet passa a ser a copia fria)",
    )
    args = ap.parse_args(argv)

    if config.USE_SAMPLE:
        raise SystemExit(
            "USE_SAMPLE=1 -> nao vale converter a amostra. "
            "seta USE_SAMPLE=0 e roda contra o dump completo."
        )

    if args.ano:
        meses = meses_do_ano(args.ano)
        if not meses:
            raise SystemExit(
                f"nenhum TXT de {args.ano} em {config.RAW_DIR}. roda o "
                f"download_senatran --ano {args.ano} primeiro."
            )
    else:
        meses = args.mes or [config.SENATRAN_MES]

    print(f"{len(meses)} mes(es): {', '.join(meses)}")
    for mes in meses:
        total, _, _ = converter(mes, apagar_txt=args.apagar_txt)
        print(f"ok: {total:,} linhas em {caminho_parquet(mes).name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
