"""TXT do SENATRAN -> Parquet (zstd) com pyarrow puro, sem JVM.

É o fallback do to_parquet_spark.py e a base do benchmark em docs/decisions.md.
Mesmo contrato: mesmas colunas, valores com strip e o mesmo limite de descarte.

    python -m pipeline.to_parquet --mes abril_2026
    python -m pipeline.to_parquet --ano 2026 --apagar-txt
"""

from collections.abc import Iterable, Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline import cli
from pipeline.arquivos import COLUNAS, LeituraTxt, substituir
from pipeline.erros import LayoutInvalido

ROW_GROUP = 500_000
SCHEMA = pa.schema([(c, pa.string()) for c in COLUNAS])


def _lotes(linhas: Iterable[tuple[str, ...]]) -> Iterator[pa.RecordBatch]:
    colunas: list[list[str]] = [[] for _ in COLUNAS]
    for linha in linhas:
        for coluna, valor in zip(colunas, linha, strict=True):
            coluna.append(valor)
        if len(colunas[0]) == ROW_GROUP:
            yield pa.record_batch(colunas, schema=SCHEMA)
            colunas = [[] for _ in COLUNAS]
    if colunas[0]:
        yield pa.record_batch(colunas, schema=SCHEMA)


def converter(fonte: Path, destino: Path) -> int:
    leitura = LeituraTxt(fonte)
    # grava ao lado e só troca no fim: um parquet pela metade nunca fica no lugar do bom
    tmp = destino.with_name(destino.name + ".tmp")
    total = 0
    try:
        with pq.ParquetWriter(tmp, SCHEMA, compression="zstd") as writer:
            for lote in _lotes(leitura):
                writer.write_batch(lote)
                total += lote.num_rows
        leitura.validar()
        if not total:
            raise LayoutInvalido(f"{fonte.name} não tem nenhuma linha válida")
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    substituir(tmp, destino)
    return total


def main(argv: list[str] | None = None) -> int:
    return cli.main_conversao(converter, argv)


if __name__ == "__main__":
    cli.executar(main)
