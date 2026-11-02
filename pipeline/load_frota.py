"""Carrega a frota em raw.frota_municipio, um mês por transação.

Cada mês é uma partição lógica (mes_referencia): apago o mês e insiro de novo,
então recarregar é idempotente e os outros meses ficam intactos. Leio o
Parquet do mês quando existe e o TXT quando não.

    python -m pipeline.load_frota                  # SENATRAN_MES do .env
    python -m pipeline.load_frota --mes maio_2026
    python -m pipeline.load_frota --ano 2026       # todo mês em data/raw
"""

import csv
import io
import logging
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import IO

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as ds
from psycopg2.extensions import connection

from pipeline import arquivos, cli, config, db
from pipeline.arquivos import COLUNAS, LeituraTxt
from pipeline.erros import ErroPipeline, FonteNaoEncontrada, LayoutInvalido
from pipeline.mes import Mes

log = logging.getLogger(__name__)

TABELA = "raw.frota_municipio"
COLUNAS_RAW = (*COLUNAS, "mes_referencia")
LOTE = 200_000

Lotes = Iterator[tuple[IO, int]]


def _lotes_txt(linhas: Iterable[tuple[str, ...]], mes: str) -> Lotes:
    buf, n = io.StringIO(), 0
    escritor = csv.writer(buf, delimiter="\t")
    for linha in linhas:
        escritor.writerow((*linha, mes))
        n += 1
        if n == LOTE:
            buf.seek(0)
            yield buf, n
            buf, n = io.StringIO(), 0
            escritor = csv.writer(buf, delimiter="\t")
    if n:
        buf.seek(0)
        yield buf, n


def _lotes_parquet(caminho: Path, mes: str) -> Lotes:
    opcoes = pacsv.WriteOptions(include_header=False, delimiter="\t")
    for lote in ds.dataset(caminho, format="parquet").to_batches(columns=list(COLUNAS)):
        if not lote.num_rows:
            continue
        tabela = pa.Table.from_batches([lote]).append_column(
            "mes_referencia", pa.array([mes] * lote.num_rows, pa.string())
        )
        buf = io.BytesIO()
        pacsv.write_csv(tabela, buf, opcoes)
        buf.seek(0)
        yield buf, lote.num_rows


def carregar_mes(conn: connection, mes: Mes, fonte: Path) -> int:
    if not fonte.exists():
        raise FonteNaoEncontrada(f"{fonte} não existe (rodou o download_senatran?)")
    log.info("%s: carregando %s", mes, fonte.name)

    leitura = None if fonte.suffix == ".parquet" else LeituraTxt(fonte)
    lotes = _lotes_txt(leitura, str(mes)) if leitura else _lotes_parquet(fonte, str(mes))

    # `with conn` faz rollback em qualquer erro: o mês antigo só some se o novo entrar inteiro
    with conn, conn.cursor() as cur:
        cur.execute(f"delete from {TABELA} where mes_referencia = %s", (str(mes),))
        total = 0
        for buf, n in lotes:
            db.copiar(cur, TABELA, COLUNAS_RAW, buf)
            total += n
        if leitura:
            leitura.validar()
        if not total:
            raise LayoutInvalido(f"{fonte.name} não tem nenhuma linha válida")
    return total


def main(argv: list[str] | None = None) -> int:
    args = cli.parser_meses("Carrega a frota no schema raw").parse_args(argv)

    if config.USE_SAMPLE:
        if args.ano:
            raise ErroPipeline("--ano não combina com USE_SAMPLE=1: a amostra é de um mês só")
        fontes = [(m, config.AMOSTRA_FROTA) for m in args.mes or [cli.mes_padrao()]]
    elif args.ano:
        meses = arquivos.meses_em_disco(args.ano)
        if not meses:
            raise FonteNaoEncontrada(f"nenhum mês de {args.ano} em {config.RAW_DIR}")
        fontes = [(m, arquivos.escolher_fonte(m)) for m in meses]
    else:
        fontes = [(m, arquivos.escolher_fonte(m)) for m in args.mes or [cli.mes_padrao()]]

    with db.conectar() as conn:
        db.aplicar_ddl(conn)
        for mes, fonte in fontes:
            total = carregar_mes(conn, mes, fonte)
            log.info("%s: %s linhas em %s", mes, f"{total:,}", TABELA)
    return 0


if __name__ == "__main__":
    cli.executar(main)
