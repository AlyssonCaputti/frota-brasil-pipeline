"""Converte o TXT da frota SENATRAN em Parquet via PySpark.

Producer oficial do parquet mensal - troca to_parquet.py (pandas/pyarrow)
como caminho de producao. Antes de virar isso, foi medido contra ele: mais
rapido mesmo com poucos nucleos (~1.5x em local[2], que e mais perto do que
um runner de CI/Airflow tem; ~4x com os 28 nucleos desta maquina de dev).
Custo real aceito nessa troca: dependencia de JVM, tuning de heap (o job
avisa sobre pressao de memoria default no volume mensal), e o setup de
HADOOP_HOME/winutils.exe que o Windows exige (Linux nao precisa - o runner
do CI e Airflow em producao ficam livres dessa parte). Numeros e o resto do
raciocinio em docs/decisions.md.

Grava no MESMO caminho que to_parquet.py usava (config.RAW_DIR /
frota_municipio_{mes}.parquet), so que como diretorio de part-files, que e o
formato nativo do Spark - forcar 1 arquivo so via coalesce(1) foi tentado
primeiro e serializou a escrita, derrubando o tempo de 23-30s pra 131.8s
(pior que o to_parquet.py original). load_frota.py le com pyarrow.dataset,
que abre arquivo unico ou diretorio de forma transparente, entao nao precisa
saber qual dos dois converters gerou o parquet.

to_parquet.py fica no repo como estava, testado e funcional - nao foi
removido, so deixou de ser o caminho que o Makefile/download_senatran chamam.

Uso:
    python -m pipeline.to_parquet_spark                  # mes do .env
    python -m pipeline.to_parquet_spark --mes maio_2026
    python -m pipeline.to_parquet_spark --ano 2026       # todo mes com TXT em disco
    python -m pipeline.to_parquet_spark --ano 2026 --apagar-txt
"""

import argparse
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from pipeline import config, to_parquet

# mesmas 5 colunas, mesmo contrato do to_parquet.py e do raw_ddl.sql - tudo
# string, cast e limpeza ficam pro dbt.
CAMPOS = ["uf", "municipio", "marca_modelo", "ano_fabricacao", "qtd_veiculos"]
SCHEMA = StructType([StructField(c, StringType(), True) for c in CAMPOS])


def converter(mes: str, apagar_txt=False, spark=None):
    """TXT -> Parquet (1 arquivo) via Spark. Devolve (linhas, descartadas, segundos)."""
    fonte = config.caminho_frota(mes)
    if not fonte.exists():
        raise SystemExit(
            f"TXT nao encontrado pro mes {mes}: {fonte} "
            f"(rodou o download_senatran?)"
        )

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
    # de 5 campos, joga fora - conto quanto foi descartado comparando com o
    # total de linhas do arquivo (mesmo espirito do load_frota.py: nunca
    # descartar calado).
    bruto = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("mode", "DROPMALFORMED")
        .schema(SCHEMA)
        .csv(str(fonte))
    )
    total = bruto.count()
    linhas_arquivo = spark.read.text(str(fonte)).count() - 1
    descartadas = linhas_arquivo - total

    destino = to_parquet.caminho_parquet(mes)
    destino.parent.mkdir(parents=True, exist_ok=True)
    # se um to_parquet.py (arquivo unico) rodou nesse mes antes, tem um
    # arquivo no caminho onde o Spark precisa criar um diretorio - mode
    # overwrite do Spark so sabe sobrescrever diretorio, nao trocar arquivo
    # por diretorio.
    if destino.exists() and not destino.is_dir():
        destino.unlink()

    # grava como diretorio de part-files (formato nativo do Spark, paralelo).
    # load_frota.py le com pyarrow.dataset, que abre arquivo unico (to_parquet.py)
    # ou diretorio (aqui) do mesmo jeito.
    bruto.write.mode("overwrite").option("compression", "zstd").parquet(str(destino))

    dt = time.time() - t0
    print(f"{mes}: {total:,} linhas ({descartadas:,} descartada(s)) -> {destino.name}")
    print(f"  tempo: {dt:.1f}s")

    if apagar_txt:
        # so depois do replace, com o parquet inteiro no lugar final
        fonte.unlink()
        print(f"  apagei {fonte.name}")

    if proprio:
        spark.stop()
    return total, descartadas, dt


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

    if args.ano:
        meses = to_parquet.meses_do_ano(args.ano)
        if not meses:
            raise SystemExit(
                f"nenhum TXT de {args.ano} em {config.RAW_DIR}. roda o "
                f"download_senatran --ano {args.ano} primeiro."
            )
    else:
        meses = args.mes or [config.SENATRAN_MES]

    spark = SparkSession.builder.appName("to_parquet_spark").master("local[*]").getOrCreate()
    try:
        for mes in meses:
            converter(mes, apagar_txt=args.apagar_txt, spark=spark)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
