"""Baixa o dump de frota por municipio do SENATRAN via CKAN.

O portal muda a URL do arquivo todo mes, entao a gente resolve pela API
do CKAN em vez de hardcodar o link. Procura no dataset o resource que bate
com o mes de referencia (SENATRAN_MES) e cujo nome tem 'municipio'.

Uso:
    python -m pipeline.download_senatran           # baixa o mes do .env
    python -m pipeline.download_senatran --mes maio_2026
"""
import argparse
import sys
import zipfile

import requests

from pipeline import config


def achar_resource_url(mes: str) -> str:
    r = requests.get(
        f"{config.CKAN_BASE}/package_show",
        params={"id": config.CKAN_DATASET_ID},
        timeout=60,
    )
    r.raise_for_status()
    resources = r.json()["result"]["resources"]

    alvo = mes.lower().replace("_", " ")  # "abril 2026"
    candidatos = [
        res for res in resources
        if "municipio" in res["name"].lower()
        and alvo in res["name"].lower().replace("_", " ")
    ]
    if not candidatos:
        nomes = "\n  - ".join(sorted(res["name"] for res in resources))
        raise SystemExit(
            f"nao achei resource de municipio pro mes '{mes}'.\n"
            f"resources disponiveis:\n  - {nomes}"
        )
    # se tiver mais de um, pega o zip
    candidatos.sort(key=lambda res: res.get("format", "").lower() != "zip")
    return candidatos[0]["url"]


def baixar(url: str, destino):
    destino.parent.mkdir(parents=True, exist_ok=True)
    print(f"baixando {url}")
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
                    print(f"\r  {baixado/1e6:.0f}/{total/1e6:.0f} MB ({pct:.0f}%)",
                          end="", flush=True)
        print()
    return destino


def descompactar(zip_path):
    with zipfile.ZipFile(zip_path) as z:
        # o zip tem um unico TXT dentro
        nome_txt = z.namelist()[0]
        z.extractall(zip_path.parent)
    return zip_path.parent / nome_txt


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mes", default=config.SENATRAN_MES)
    args = ap.parse_args()

    if config.USE_SAMPLE:
        print("USE_SAMPLE=1 -> pulando download, usando data/samples/. "
              "seta USE_SAMPLE=0 pra baixar o dump completo.")
        return 0

    url = achar_resource_url(args.mes)
    zip_path = config.RAW_DIR / f"frota_municipio_{args.mes}.zip"
    baixar(url, zip_path)
    txt = descompactar(zip_path)
    print(f"pronto: {txt}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
