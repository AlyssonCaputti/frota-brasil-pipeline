"""Dimensiona particao e cluster do BigQuery pelo custo por byte lido.

O BigQuery on-demand cobra pelos bytes que a consulta le, e le so as colunas
do SELECT dentro das particoes que sobraram do WHERE. Parquet e colunar e ja
guarda o tamanho de cada coluna, entao da pra medir cada cenario de consulta
antes de existir um projeto GCP.

Proxy, nao equivalencia: o BigQuery armazena em Capacitor, nao em Parquet, e
cobra um minimo de 10 MB por tabela referenciada. Os numeros absolutos vao
diferir; a ordem de grandeza e a razao entre cenarios, que e o que decide
particao e cluster, se sustentam.

Uso:
    python -m pipeline.dimensiona_bq
    python -m pipeline.dimensiona_bq --meses 36
"""

import argparse
import sys

import pyarrow.compute as pc
import pyarrow.parquet as pq

from pipeline import config, to_parquet

# on-demand, us$/TiB escaneado. varia por regiao - southamerica-east1 e mais
# caro que us-central1, entao isso aqui e piso.
USD_POR_TIB = 6.25
TIB = 1024**4

# consultas que o mart tem que responder, e o que cada uma toca de verdade
CENARIOS = [
    (
        "frota nacional por marca, 1 mes",
        ["marca_modelo", "qtd_veiculos"],
        1,
    ),
    (
        "serie de 1 marca ao longo de 3 anos",
        ["marca_modelo", "qtd_veiculos"],
        None,
    ),
    (
        "ranking de municipio, 1 mes",
        ["municipio", "marca_modelo", "qtd_veiculos"],
        1,
    ),
    (
        "dump completo (select *)",
        None,
        None,
    ),
]


def bytes_por_coluna(caminho):
    """{coluna: bytes logicos} na conta do BigQuery: STRING = 2 + len(utf8).

    Nao da pra usar o total_uncompressed_size do rodape: com dictionary
    encoding ele mede os indices do dicionario, nao as strings expandidas,
    e subestima a fatura em varias vezes. O jeito certo e somar o tamanho
    real dos valores, que e o que o on-demand cobra.
    """
    total = {}
    for lote in pq.ParquetFile(caminho).iter_batches(batch_size=500_000):
        for nome, coluna in zip(lote.schema.names, lote.columns):
            soma = pc.sum(pc.binary_length(coluna)).as_py() or 0
            total[nome] = total.get(nome, 0) + soma + 2 * len(coluna)
    return total


def fatia_por_marca(caminho):
    """[(marca, fatia das linhas)] em ordem decrescente.

    A marca e o 1o token de marca_modelo, antes da "/" - mesma ideia do
    stg_frota, sem o de-para (aqui so interessa a ordem de grandeza da poda).
    """
    contagem = {}
    total = 0
    for lote in pq.ParquetFile(caminho).iter_batches(
        batch_size=500_000, columns=["marca_modelo"]
    ):
        for partes in pc.split_pattern(lote.column(0), "/").to_pylist():
            marca = partes[0] if partes else ""
            contagem[marca] = contagem.get(marca, 0) + 1
            total += 1
    return sorted(
        ((m, n / total) for m, n in contagem.items()), key=lambda kv: -kv[1]
    )


def custo(n_bytes):
    return n_bytes / TIB * USD_POR_TIB


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--mes", default=config.SENATRAN_MES)
    ap.add_argument(
        "--meses", type=int, default=36, help="tamanho do historico simulado"
    )
    args = ap.parse_args(argv)

    caminho = to_parquet.caminho_parquet(args.mes)
    if not caminho.exists():
        raise SystemExit(
            f"parquet nao encontrado: {caminho}\n"
            f"roda: python -m pipeline.to_parquet --mes {args.mes}"
        )

    colunas = bytes_por_coluna(caminho)
    md = pq.ParquetFile(caminho).metadata
    mes_bytes = sum(colunas.values())
    hist_bytes = mes_bytes * args.meses

    em_disco = caminho.stat().st_size
    print(f"medido em {caminho.name}: {md.num_rows:,} linhas")
    print(f"  {em_disco / 1e6:.0f} MB em disco (zstd) -> "
          f"{mes_bytes / 1e6:.0f} MB logicos, que e o que o on-demand cobra")
    print(f"historico simulado: {args.meses} meses, "
          f"{hist_bytes / 1e9:.1f} GB logicos, "
          f"{md.num_rows * args.meses / 1e6:.0f}M linhas")
    print()

    print("bytes logicos por coluna (1 mes):")
    for nome, n in sorted(colunas.items(), key=lambda kv: -kv[1]):
        print(f"  {nome:<16} {n / 1e6:>8.1f} MB  {100 * n / mes_bytes:>5.1f}%")
    print()

    print(f"{'consulta':<38}{'sem particao':>14}{'com particao':>14}{'economia':>10}")
    for nome, cols, meses_lidos in CENARIOS:
        largura = mes_bytes if cols is None else sum(colunas[c] for c in cols)
        # sem particao a consulta varre o historico inteiro; com particao, so
        # os meses do filtro (None = a consulta realmente precisa de todos).
        sem = largura * args.meses
        com = largura * (meses_lidos or args.meses)
        economia = "-" if sem == com else f"{100 * (1 - com / sem):.0f}%"
        print(
            f"{nome:<38}{sem / 1e9:>11.1f} GB{com / 1e9:>11.1f} GB{economia:>10}"
        )
    print()

    # Cluster: a particao nao ajuda a consulta que atravessa o historico
    # inteiro atras de uma marca. Clusterizando por marca o BigQuery poda
    # blocos, e o que sobra e proporcional a fatia da marca na tabela.
    fatias = fatia_por_marca(caminho)
    print("cluster por marca - fatia da tabela que sobra no filtro:")
    for marca, fatia in fatias[:5]:
        print(f"  marca = {marca!r:<14} {100 * fatia:>6.2f}% das linhas")
    mediana = fatias[len(fatias) // 2][1]
    print(f"  {'marca mediana':<22} {100 * mediana:>6.2f}%  "
          f"({len(fatias):,} marcas distintas)")
    print()

    serie = sum(colunas[c] for c in ("marca_modelo", "qtd_veiculos"))
    serie_hist = serie * args.meses
    print("serie de 1 marca em 36 meses (onde a particao nao ajuda):")
    print(f"  so particionado:        {serie_hist / 1e9:>6.1f} GB  "
          f"US$ {custo(serie_hist):.4f}")
    print(f"  + cluster (marca top):  {serie_hist * fatias[0][1] / 1e9:>6.1f} GB  "
          f"US$ {custo(serie_hist * fatias[0][1]):.4f}")
    print()

    varredura_total = mes_bytes * args.meses
    mes_2col = sum(colunas[c] for c in ("marca_modelo", "qtd_veiculos"))
    print(f"custo on-demand a US$ {USD_POR_TIB}/TiB:")
    print(f"  varredura completa:              US$ {custo(varredura_total):.2f}")
    print(f"  1 mes, 2 colunas (particionado): US$ {custo(mes_2col):.4f}")
    print(f"  mesma consulta sem particao:     "
          f"US$ {custo(mes_2col * args.meses):.4f}")
    print()
    # Em consulta avulsa isso e troco. O que decide e a frequencia: um painel
    # que roda a mesma consulta o dia inteiro multiplica a diferenca.
    print("a mesma consulta num painel, 1.000x/dia:")
    print(f"  particionado:  US$ {custo(mes_2col) * 1000:>7.2f}/dia  "
          f"US$ {custo(mes_2col) * 30000:>8.2f}/mes")
    print(f"  sem particao:  US$ {custo(mes_2col * args.meses) * 1000:>7.2f}/dia  "
          f"US$ {custo(mes_2col * args.meses) * 30000:>8.2f}/mes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
