"""Sobe a frota de um mês (Parquet) para raw.frota_municipio no BigQuery.

Mesmo contrato do load_frota: tudo texto, apago o mês e insiro de novo. Se cair
no meio, rodar de novo resolve. O BigQuery Sandbox não aceita DELETE, então
recarregar um mês exige o projeto com billing.

    GCP_PROJECT=meu-projeto python -m pipeline.load_bq --mes abril_2026
"""

import io
import logging
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from google.cloud import bigquery

from pipeline import arquivos, cli
from pipeline.arquivos import COLUNAS
from pipeline.erros import ErroPipeline, FonteNaoEncontrada
from pipeline.mes import Mes

log = logging.getLogger(__name__)

COLUNAS_RAW = (*COLUNAS, "mes_referencia")
SCHEMA = [bigquery.SchemaField(c, "STRING") for c in COLUNAS_RAW]
LOTE = 2_000_000  # linhas por load job; load job é grátis, o limite é memória


def preparar(cliente: bigquery.Client, tabela: str, local: str) -> None:
    dataset = bigquery.Dataset(tabela.rsplit(".", 1)[0])
    dataset.location = local
    cliente.create_dataset(dataset, exists_ok=True)
    cliente.create_table(bigquery.Table(tabela, schema=SCHEMA), exists_ok=True)


def carregar_mes(cliente: bigquery.Client, tabela: str, mes: Mes, fonte: Path) -> int:
    if not fonte.exists():
        raise FonteNaoEncontrada(f"{fonte} não existe; o load_bq só lê Parquet")

    parametro = bigquery.ScalarQueryParameter("mes", "STRING", str(mes))
    cliente.query(
        f"delete from `{tabela}` where mes_referencia = @mes",
        job_config=bigquery.QueryJobConfig(query_parameters=[parametro]),
    ).result()

    config_load = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=SCHEMA,
    )
    total = 0
    for lote in ds.dataset(fonte, format="parquet").to_batches(
        columns=list(COLUNAS), batch_size=LOTE
    ):
        tabela_arrow = pa.Table.from_batches([lote]).append_column(
            "mes_referencia", pa.array([str(mes)] * lote.num_rows, pa.string())
        )
        buf = io.BytesIO()
        pq.write_table(tabela_arrow, buf)
        buf.seek(0)
        cliente.load_table_from_file(buf, tabela, job_config=config_load).result()
        total += lote.num_rows
    return total


def main(argv: list[str] | None = None) -> int:
    args = cli.parser_meses("Carrega a frota (Parquet) no BigQuery").parse_args(argv)
    projeto = os.getenv("GCP_PROJECT")
    if not projeto:
        raise ErroPipeline("defina GCP_PROJECT")

    tabela = f"{projeto}.{os.getenv('BQ_RAW_DATASET', 'raw')}.frota_municipio"
    local = os.getenv("BQ_LOCATION", "southamerica-east1")
    cliente = bigquery.Client(project=projeto, location=local)
    preparar(cliente, tabela, local)

    meses = arquivos.meses_em_disco(args.ano) if args.ano else args.mes or [cli.mes_padrao()]
    for mes in meses:
        total = carregar_mes(cliente, tabela, mes, arquivos.caminho_parquet(mes))
        log.info("%s: %s linhas em %s", mes, f"{total:,}", tabela)
    return 0


if __name__ == "__main__":
    cli.executar(main)
