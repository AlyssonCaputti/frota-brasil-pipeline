"""Baixa o dump de frota por municipio do SENATRAN via CKAN.

O portal muda a URL do arquivo todo mes, entao a gente resolve pela API
do CKAN em vez de hardcodar o link. O campo "name" dos resources vem
truncado igual para todos ("Registro Nacional de Veiculos Automotores -..."),
por isso o match sai pela url, que carrega o nome de arquivo real.

Uma chamada no package_show ja lista todo mes publicado, entao --ano nao
custa request a mais - e mes ja baixado e pulado. Cada arquivo tem retry
com backoff porque o portal corta conexao no meio do download.

Uso:
    python -m pipeline.download_senatran                # baixa o mes do .env
    python -m pipeline.download_senatran --mes maio_2026
    python -m pipeline.download_senatran --ano 2026     # todo mes publicado do ano
"""

import argparse
import re
import sys
import time
import zipfile

import requests

from pipeline import config

# so .zip: os dumps de 2013-2018 vem em .rar, que o zipfile nao abre. o nome
# do mes vem do proprio padrao pra ja validar e separar mes/ano.
PADRAO_MES = re.compile(
    r"_municipio_marca_e_modelo_ano_(" + "|".join(config.MESES) + r")_(\d{4})\.zip$"
)

PAUSA_MESES = 3  # seg entre downloads, pra nao socar o portal no backfill do ano


def meses_publicados():
    """{"abril_2026": url} dos dumps de municipio, em ordem cronologica."""
    r = requests.get(
        f"{config.CKAN_BASE}/package_show",
        params={"id": config.CKAN_DATASET_ID},
        timeout=60,
    )
    r.raise_for_status()

    achados = []
    for res in r.json()["result"]["resources"]:
        m = PADRAO_MES.search(res["url"].lower())
        if m:
            nome, ano = m.group(1), m.group(2)
            ordem = (int(ano), config.MESES.index(nome))
            achados.append((ordem, f"{nome}_{ano}", res["url"]))
    return {mes: url for _, mes, url in sorted(achados)}


def _stream(url, destino):
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        baixado = 0
        with open(destino, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
                baixado += len(chunk)
                if total:
                    pct = 100 * baixado / total
                    print(
                        f"\r  {baixado / 1e6:.0f}/{total / 1e6:.0f} MB ({pct:.0f}%)",
                        end="",
                        flush=True,
                    )
        print()
    return destino


def baixar(url: str, destino, tentativas=4):
    destino.parent.mkdir(parents=True, exist_ok=True)
    print(f"baixando {url}")
    for i in range(tentativas):
        try:
            return _stream(url, destino)
        except requests.RequestException as e:
            # o portal corta a conexao no meio de um arquivo de ~130MB de vez
            # em quando (IncompleteRead). recomeca o arquivo do zero - Range
            # aqui nao e confiavel - com o mesmo backoff do fipe_client.
            espera = 2 ** i
            print(f"\n  caiu em {type(e).__name__}, tentando de novo em {espera}s")
            time.sleep(espera)
    raise RuntimeError(f"desisti de {url} depois de {tentativas} tentativas")


def descompactar(zip_path):
    with zipfile.ZipFile(zip_path) as z:
        # o zip tem um unico TXT dentro
        nome_txt = z.namelist()[0]
        z.extractall(zip_path.parent)
    return zip_path.parent / nome_txt


def main():
    ap = argparse.ArgumentParser()
    grupo = ap.add_mutually_exclusive_group()
    grupo.add_argument("--mes")
    grupo.add_argument("--ano", help="baixa todo mes publicado do ano")
    args = ap.parse_args()

    if config.USE_SAMPLE:
        print(
            "USE_SAMPLE=1 -> pulando download, usando data/samples/. "
            "seta USE_SAMPLE=0 pra baixar o dump completo."
        )
        return 0

    publicados = meses_publicados()
    if args.ano:
        meses = [m for m in publicados if m.endswith(f"_{args.ano}")]
        if not meses:
            raise SystemExit(
                f"nenhum mes publicado pra {args.ano}.\n"
                "meses disponiveis:\n  - " + "\n  - ".join(publicados)
            )
    else:
        mes = args.mes or config.SENATRAN_MES
        if mes not in publicados:
            raise SystemExit(
                f"nao achei resource de municipio pro mes '{mes}'.\n"
                "meses disponiveis:\n  - " + "\n  - ".join(publicados)
            )
        meses = [mes]

    print(f"{len(meses)} mes(es): {', '.join(meses)}")
    baixados = 0
    for mes in meses:
        txt = config.caminho_frota(mes)
        if txt.exists():
            print(f"{mes}: {txt.name} ja existe, pulando")
            continue
        if baixados:
            time.sleep(PAUSA_MESES)
        zip_path = config.RAW_DIR / f"frota_municipio_{mes}.zip"
        baixar(publicados[mes], zip_path)
        print(f"pronto: {descompactar(zip_path)}")
        baixados += 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
