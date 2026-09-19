"""Converte o TXT da frota em Parquet particionado por mes_referencia.

O TXT do SENATRAN e caro de guardar e de reler: ~1.2GB por mes, sem tipo e
sem compressao. Em Parquet com zstd a mesma coisa cabe numa fracao disso,
porque uf/municipio/marca_modelo sao poucos valores distintos repetidos
milhoes de vezes (dictionary encoding resolve).

Mantem as 5 colunas como texto, igual a raw.frota_municipio: o contrato com
o staging do dbt continua o mesmo, a limpeza segue sendo la. O mes vai no
nome do diretorio (mes_referencia=abril_2026), que e o layout hive que o
BigQuery espera numa external table.

Diferente do load, aqui linha torta e so contada e pulada: o piso de
descarte que aborta a carga vive no load_frota, que e onde o dado vira
tabela. Isso aqui e conversao de formato da origem.

Uso:
    python -m pipeline.to_parquet                   # mes do .env
    python -m pipeline.to_parquet --mes maio_2026
    python -m pipeline.to_parquet --ano 2026        # todo mes ja baixado do ano
"""

import argparse
import csv
import sys

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline import config

# quantas linhas por row group. 500k deixa o arquivo com poucos grupos
# grandes, que e o que faz o leitor pular bloco inteiro no predicate.
LINHAS_POR_GRUPO = 500_000

ESQUEMA = pa.schema(
    [
        ("uf", pa.string()),
        ("municipio", pa.string()),
        ("marca_modelo", pa.string()),
        ("ano_fabricacao", pa.string()),
        ("qtd_veiculos", pa.string()),
    ]
)


def destino_parquet(mes):
    """Diretorio hive do mes, do jeito que o BigQuery le external table."""
    return config.PARQUET_DIR / f"mes_referencia={mes}" / "frota.parquet"


def _grupos(reader, descartes):
    """Le o csv e vai devolvendo RecordBatch de LINHAS_POR_GRUPO linhas."""
    colunas = [[] for _ in range(5)]
    n = 0
    for row in reader:
        if len(row) != 5:
            descartes.append(row)
            continue
        for i, valor in enumerate(row):
            colunas[i].append(valor.strip())
        n += 1
        if n >= LINHAS_POR_GRUPO:
            yield pa.RecordBatch.from_arrays(colunas, schema=ESQUEMA)
            colunas = [[] for _ in range(5)]
            n = 0
    if n:
        yield pa.RecordBatch.from_arrays(colunas, schema=ESQUEMA)


def converter_mes(mes):
    """Converte um mes e devolve (linhas, bytes do txt, bytes do parquet)."""
    fonte = config.caminho_frota(mes)
    if not fonte.exists():
        raise SystemExit(f"fonte nao encontrada: {fonte} (rodou o download?)")

    destino = destino_parquet(mes)
    destino.parent.mkdir(parents=True, exist_ok=True)
    print(f"convertendo {fonte.name} -> {destino.parent.name}/{destino.name}")

    total = 0
    descartes = []
    with open(fonte, encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=";")
        next(reader)  # header
        escritor = pq.ParquetWriter(destino, ESQUEMA, compression="zstd")
        try:
            for grupo in _grupos(reader, descartes):
                escritor.write_batch(grupo)
                total += grupo.num_rows
                print(f"\r  {total:,} linhas", end="", flush=True)
        finally:
            escritor.close()

    if descartes:
        print(f"\n  {len(descartes):,} linha(s) pulada(s) por numero de campos != 5")

    txt_bytes = fonte.stat().st_size
    pq_bytes = destino.stat().st_size
    print(
        f"\n  {total:,} linhas | {txt_bytes / 1e9:.2f} GB -> "
        f"{pq_bytes / 1e9:.2f} GB ({txt_bytes / pq_bytes:.1f}x menor)"
    )
    return total, txt_bytes, pq_bytes


def main():
    ap = argparse.ArgumentParser()
    grupo = ap.add_mutually_exclusive_group()
    grupo.add_argument("--mes", action="append", help="pode repetir")
    grupo.add_argument("--ano", help="converte todo mes do ano ja baixado")
    args = ap.parse_args()

    if args.ano:
        meses = [f"{m}_{args.ano}" for m in config.MESES]
        meses = [m for m in meses if config.caminho_frota(m).exists()]
        if not meses:
            raise SystemExit(
                f"nenhum TXT de {args.ano} em {config.RAW_DIR}. roda o "
                f"download_senatran --ano {args.ano} primeiro."
            )
    else:
        meses = args.mes or [config.SENATRAN_MES]

    linhas = txt = parq = 0
    for mes in meses:
        n, t, p = converter_mes(mes)
        linhas += n
        txt += t
        parq += p

    if len(meses) > 1:
        print(
            f"\ntotal: {linhas:,} linhas em {len(meses)} meses | "
            f"{txt / 1e9:.1f} GB -> {parq / 1e9:.1f} GB "
            f"({txt / parq:.1f}x menor)"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
