"""Dimensiona partição e cluster do BigQuery pelo custo por byte lido.

O on-demand cobra os bytes lógicos das colunas lidas, dentro das partições que
sobram do WHERE. Meço isso no Parquet real do mês. É um proxy (o BigQuery usa
Capacitor e cobra mínimo de 10 MB por tabela), mas a razão entre cenários, que
é o que decide partição e cluster, se mantém.

    python -m pipeline.dimensiona_bq --mes abril_2026 --meses 36
"""

import argparse
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.dataset as ds

from pipeline import arquivos, cli
from pipeline.erros import FonteNaoEncontrada
from pipeline.mes import Mes

USD_POR_TIB = 6.25  # us-central1; southamerica-east1 é mais caro, então é piso
TIB = 1024**4
LOTE = 500_000

# (consulta, colunas lidas ou None = todas, meses lidos ou None = histórico todo)
CENARIOS = [
    ("frota nacional por marca, 1 mês", ["marca_modelo", "qtd_veiculos"], 1),
    ("série de 1 marca no histórico", ["marca_modelo", "qtd_veiculos"], None),
    ("ranking de município, 1 mês", ["municipio", "marca_modelo", "qtd_veiculos"], 1),
    ("select *", None, None),
]


def bytes_por_coluna(dataset: ds.Dataset) -> dict[str, int]:
    """Bytes lógicos como o BigQuery conta um STRING: 2 + tamanho em UTF-8.

    O total_uncompressed_size do rodapé não serve: com dicionário ele mede os
    índices, não as strings, e subestima a fatura várias vezes.
    """
    total: dict[str, int] = {}
    for lote in dataset.to_batches(batch_size=LOTE):
        for nome, coluna in zip(lote.schema.names, lote.columns, strict=True):
            soma = pc.sum(pc.binary_length(coluna)).as_py() or 0
            total[nome] = total.get(nome, 0) + soma + 2 * len(coluna)
    return total


def fatia_por_marca(dataset: ds.Dataset) -> list[tuple[str, float]]:
    """Fração das linhas de cada marca (1o token de marca_modelo), maior primeiro."""
    contagem: dict[str, int] = {}
    for lote in dataset.to_batches(batch_size=LOTE, columns=["marca_modelo"]):
        for partes in pc.split_pattern(lote.column(0), "/").to_pylist():
            marca = partes[0] if partes else ""
            contagem[marca] = contagem.get(marca, 0) + 1
    total = sum(contagem.values())
    return sorted(((m, n / total) for m, n in contagem.items()), key=lambda kv: -kv[1])


def custo(n_bytes: float) -> float:
    return n_bytes / TIB * USD_POR_TIB


def tamanho_em_disco(dataset: ds.Dataset) -> int:
    return sum(Path(f).stat().st_size for f in dataset.files)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Custo de consulta no BigQuery, medido no Parquet")
    ap.add_argument("--mes", type=Mes.parse, default=cli.mes_padrao())
    ap.add_argument("--meses", type=int, default=36, help="tamanho do histórico simulado")
    args = ap.parse_args(argv)

    caminho = arquivos.caminho_parquet(args.mes)
    if not caminho.exists():
        raise FonteNaoEncontrada(f"{caminho} não existe; rode o to_parquet_spark antes")

    dataset = ds.dataset(caminho, format="parquet")
    colunas = bytes_por_coluna(dataset)
    mes_bytes = sum(colunas.values())
    linhas = dataset.count_rows()

    print(f"{caminho.name}: {linhas:,} linhas")
    print(
        f"  {tamanho_em_disco(dataset) / 1e6:.0f} MB em disco -> {mes_bytes / 1e6:.0f} MB lógicos"
    )
    print(f"  histórico de {args.meses} meses: {mes_bytes * args.meses / 1e9:.1f} GB lógicos\n")

    print(f"{'consulta':<36}{'sem partição':>14}{'com partição':>14}{'economia':>10}")
    for nome, cols, meses_lidos in CENARIOS:
        largura = mes_bytes if cols is None else sum(colunas[c] for c in cols)
        sem = largura * args.meses
        com = largura * (meses_lidos or args.meses)
        economia = "-" if sem == com else f"{100 * (1 - com / sem):.0f}%"
        print(f"{nome:<36}{sem / 1e9:>11.1f} GB{com / 1e9:>11.1f} GB{economia:>10}")

    fatias = fatia_por_marca(dataset)
    print("\ncluster por marca, fração da tabela que sobra no filtro:")
    for marca, fatia in fatias[:5]:
        print(f"  {marca!r:<16} {100 * fatia:>6.2f}%")
    print(f"  marca mediana    {100 * fatias[len(fatias) // 2][1]:>6.2f}% ({len(fatias):,} marcas)")

    serie = (colunas["marca_modelo"] + colunas["qtd_veiculos"]) * args.meses
    com_cluster = serie * fatias[0][1]
    print(f"\nsérie da maior marca em {args.meses} meses:")
    print(f"  só partição:  {serie / 1e9:>6.1f} GB  US$ {custo(serie):.4f}")
    print(f"  + cluster:    {com_cluster / 1e9:>6.1f} GB  US$ {custo(com_cluster):.4f}")

    um_mes = colunas["marca_modelo"] + colunas["qtd_veiculos"]
    print("\nmesma consulta de 1 mês num painel, 1.000x/dia:")
    print(f"  particionado:  US$ {custo(um_mes) * 30_000:>9.2f}/mês")
    print(f"  sem partição:  US$ {custo(um_mes * args.meses) * 30_000:>9.2f}/mês")
    return 0


if __name__ == "__main__":
    cli.executar(main)
