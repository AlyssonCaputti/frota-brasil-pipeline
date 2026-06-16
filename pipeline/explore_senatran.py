"""Exploracao rapida do dump SENATRAN: encoding, separador, colunas,
contagem de linhas e amostra. Roda contra a amostra por padrao.

Foi o primeiro script que escrevi olhando o arquivo - deixei aqui porque
serve de sanity-check antes de carregar (dump muda de layout de vez em quando).
"""
import csv
import sys
from collections import Counter

from pipeline import config


def caminho_fonte():
    if config.USE_SAMPLE:
        return config.SAMPLES_DIR / "senatran_frota_sample.csv"
    return config.RAW_DIR / (
        f"i_frota_por_uf_municipio_marca_e_modelo_ano_{config.SENATRAN_MES}.TXT"
    )


def main():
    fonte = caminho_fonte()
    if not fonte.exists():
        raise SystemExit(f"fonte nao encontrada: {fonte}")

    print(f"fonte: {fonte}")
    print(f"tamanho: {fonte.stat().st_size / 1e6:.1f} MB")

    # utf-8-sig: o dump vem com BOM no comeco do header
    with open(fonte, encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)
        print(f"colunas ({len(header)}): {header}")

        n = 0
        ufs = Counter()
        anos_estranhos = Counter()
        amostra = []
        for row in reader:
            n += 1
            if len(row) != len(header):
                continue
            ufs[row[0]] += 1
            ano = row[3].strip()
            if not ano.isdigit():
                anos_estranhos[ano] += 1
            if len(amostra) < 5:
                amostra.append(row)

    print(f"linhas de dados: {n:,}")
    print(f"UFs distintas: {len(ufs)}")
    print("top 5 UFs por linhas:")
    for uf, c in ufs.most_common(5):
        print(f"  {uf}: {c:,}")
    if anos_estranhos:
        print("valores de ano nao-numericos (vao ser descartados no staging):")
        for v, c in anos_estranhos.most_common():
            print(f"  {v!r}: {c}")
    print("amostra:")
    for row in amostra:
        print(f"  {row}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
