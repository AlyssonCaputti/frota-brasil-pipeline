"""TXT do SENATRAN -> Parquet com PySpark. É o conversor que a DAG e o Makefile usam.

Grava um diretório de part-files (escrita paralela). O load_frota lê com
pyarrow.dataset, que trata arquivo único e diretório do mesmo jeito.

    python -m pipeline.to_parquet_spark --mes abril_2026
    python -m pipeline.to_parquet_spark --ano 2026 --apagar-txt

No Windows o Spark precisa de HADOOP_HOME apontando para um winutils.exe.
"""

import os
import shutil
from pathlib import Path

import pyarrow.dataset as ds
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pipeline import cli
from pipeline.arquivos import COLUNAS, substituir, validar_descarte
from pipeline.erros import LayoutInvalido

SCHEMA = StructType([StructField(c, StringType()) for c in COLUNAS])


def _sessao() -> SparkSession:
    return (
        SparkSession.builder.appName("frota_parquet")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )


def converter(fonte: Path, destino: Path) -> int:
    spark = _sessao()
    df = spark.read.csv(
        str(fonte), schema=SCHEMA, sep=";", header=True, encoding="UTF-8", mode="DROPMALFORMED"
    ).select([F.coalesce(F.trim(c), F.lit("")).alias(c) for c in COLUNAS])

    tmp = destino.with_name(destino.name + ".tmp")
    try:
        df.write.mode("overwrite").option("compression", "zstd").parquet(str(tmp))
        # df.count() não serve: sem colunas pedidas o Spark nem faz o parse e não
        # descarta nada. Conto o que foi gravado de fato.
        gravadas = ds.dataset(tmp, format="parquet").count_rows()
        lidas = spark.read.text(str(fonte)).filter(F.trim("value") != "").count() - 1
        validar_descarte(lidas, lidas - gravadas, fonte.name)
        if not gravadas:
            raise LayoutInvalido(f"{fonte.name} não tem nenhuma linha válida")
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    substituir(tmp, destino)
    return gravadas


def main(argv: list[str] | None = None) -> int:
    try:
        return cli.main_conversao(converter, argv)
    finally:
        if sessao := SparkSession.getActiveSession():
            sessao.stop()


if __name__ == "__main__":
    cli.executar(main)
