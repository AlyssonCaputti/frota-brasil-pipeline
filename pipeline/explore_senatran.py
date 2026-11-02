"""Olhada rápida no dump antes de carregar: colunas, linhas, UFs e anos estranhos.

python -m pipeline.explore_senatran --mes abril_2026
"""

import argparse
import csv
from collections import Counter

from pipeline import arquivos, cli, config
from pipeline.erros import FonteNaoEncontrada
from pipeline.mes import Mes


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Resumo do TXT da frota")
    ap.add_argument("--mes", type=Mes.parse, default=cli.mes_padrao())
    args = ap.parse_args(argv)

    fonte = config.AMOSTRA_FROTA if config.USE_SAMPLE else arquivos.caminho_txt(args.mes)
    if not fonte.exists():
        raise FonteNaoEncontrada(f"{fonte} não existe")

    with open(fonte, encoding="utf-8-sig", newline="") as f:
        leitor = csv.reader(f, delimiter=";")
        cabecalho = next(leitor)
        linhas, tortas = 0, 0
        ufs: Counter[str] = Counter()
        anos_estranhos: Counter[str] = Counter()
        for linha in leitor:
            linhas += 1
            if len(linha) != len(cabecalho):
                tortas += 1
                continue
            ufs[linha[0]] += 1
            if not linha[3].strip().isdigit():
                anos_estranhos[linha[3].strip()] += 1

    print(f"{fonte.name}: {fonte.stat().st_size / 1e6:.1f} MB, {linhas:,} linhas, {tortas} tortas")
    print(f"colunas: {cabecalho}")
    print(f"{len(ufs)} UFs; maiores: {ufs.most_common(5)}")
    if anos_estranhos:
        print(f"anos não numéricos (viram null no staging): {anos_estranhos.most_common()}")
    return 0


if __name__ == "__main__":
    cli.executar(main)
