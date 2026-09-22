"""Converte o TXT da frota SENATRAN em Parquet via PySpark.

Alternativa ao to_parquet.py (pandas/pyarrow em streaming), nao substituto:
existe pra medir Spark contra o pipeline atual, nao porque o volume de hoje
(~22M linhas/mes, ~700M em 3 anos, ~1.9GB em Parquet) precise de Spark - isso
cabe inteiro na memoria de uma maquina so, que e exatamente o que to_parquet.py
ja faz em memoria constante. Numeros do benchmark e a decisao de nao trocar
o pipeline de producao por isso estao em docs/decisions.md.

Roda em local[*] (sem cluster real) - nao ha infraestrutura distribuida aqui,
e seria desonesto sugerir que ha.

Uso:
    python -m pipeline.to_parquet_spark --mes abril_2026
"""

import argparse
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from pipeline import config

# mesmas 5 colunas, mesmo contrato do to_parquet.py e do raw_ddl.sql - tudo
# string, cast e limpeza ficam pro dbt.
CAMPOS = ["uf", "municipio", "marca_modelo", "ano_fabricacao", "qtd_veiculos"]
SCHEMA = StructType([StructField(c, StringType(), True) for c in CAMPOS])


def caminho_parquet(mes: str):
    """Diretorio Parquet do mes. Spark grava varios part-files, nao 1 arquivo
    como o to_parquet.py - por isso fica num subdiretorio proprio, nao
    disputa o mesmo nome do arquivo do pipeline de producao."""
    return config.RAW_DIR / "spark" / f"frota_municipio_{mes}"


def converter(mes: str, spark=None):
    """TXT -> Parquet via Spark. Devolve (linhas, descartadas, segundos)."""
    fonte = config.caminho_frota(mes)
    if not fonte.exists():
        raise SystemExit(f"TXT nao encontrado pro mes {mes}: {fonte}")

    proprio = spark is None
    if proprio:
        spark = (
            SparkSession.builder.appName(f"to_parquet_spark_{mes}")
            .master("local[*]")
            .getOrCreate()
        )

    t0 = time.time()
    # header="true" descarta a 1a linha do arquivo (o BOM utf-8-sig entra no
    # nome da 1a coluna, mas como o schema e fixo por posicao isso nao importa).
    # DROPMALFORMED = mesmo criterio de "linha torta" do to_parquet.py: menos
    # de 5 campos, joga fora - so que aqui preciso contar separado pra saber
    # quanto foi descartado (ver abaixo).
    bruto = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("mode", "DROPMALFORMED")
        .schema(SCHEMA)
        .csv(str(fonte))
    )
    total = bruto.count()

    # linhas de dado no arquivo (sem header) menos as que sobreviveram ao
    # DROPMALFORMED = quantas foram descartadas. Mesmo espirito do
    # load_frota.py: nunca descartar calado.
    linhas_arquivo = spark.read.text(str(fonte)).count() - 1
    descartadas = linhas_arquivo - total

    destino = caminho_parquet(mes)
    bruto.write.mode("overwrite").option("compression", "zstd").parquet(str(destino))

    dt = time.time() - t0
    print(f"{mes}: {total:,} linhas ({descartadas:,} descartada(s)) -> {destino}")
    print(f"  tempo: {dt:.1f}s")

    if proprio:
        spark.stop()
    return total, descartadas, dt


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--mes", default=config.SENATRAN_MES)
    args = ap.parse_args(argv)

    if config.USE_SAMPLE:
        raise SystemExit(
            "USE_SAMPLE=1 -> nao vale rodar Spark na amostra de 75k linhas, "
            "o ponto e comparar no volume real. seta USE_SAMPLE=0."
        )

    converter(args.mes)
    return 0


if __name__ == "__main__":
    sys.exit(main())
