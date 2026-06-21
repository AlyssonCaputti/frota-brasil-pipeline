"""Cliente da API FIPE (parallelum) com cache em disco.

A API e gratuita mas tem rate limit e cai as vezes, entao:
- cacheia o catalogo inteiro num JSON (data/raw/fipe_catalogo.json);
- se USE_SAMPLE=1, usa o catalogo que ja vem versionado em data/samples.

Estrutura do catalogo:
    { "marcas":  [ {"codigo": "1", "nome": "Acura"}, ... ],
      "modelos": { "<marca_codigo>": [ {"codigo": 1, "nome": "Integra GS 1.8"}, ... ] } }
"""
import json
import time

import requests

from pipeline import config

API = "https://parallelum.com.br/fipe/api/v1/carros"
PAUSA = 0.4  # seg entre chamadas, pra nao tomar 429


def _get(url, tentativas=4):
    for i in range(tentativas):
        r = requests.get(url, timeout=30)
        if r.status_code == 429:
            espera = 2 ** i
            print(f"  429, esperando {espera}s")
            time.sleep(espera)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"desisti de {url} depois de {tentativas} tentativas")


def baixar_catalogo():
    """Baixa marcas + modelos de todas as marcas. Demora (uma call por marca)."""
    marcas = _get(f"{API}/marcas")
    modelos = {}
    for m in marcas:
        cod = m["codigo"]
        print(f"  {m['nome']}...")
        data = _get(f"{API}/marcas/{cod}/modelos")
        modelos[cod] = data.get("modelos", data)
        time.sleep(PAUSA)
    return {"marcas": marcas, "modelos": modelos}


def carregar_catalogo():
    """Retorna o catalogo, do sample ou do cache, baixando se preciso."""
    if config.USE_SAMPLE:
        path = config.SAMPLES_DIR / "fipe_catalogo.json"
        return json.loads(path.read_text(encoding="utf-8"))

    cache = config.RAW_DIR / "fipe_catalogo.json"
    if cache.exists():
        print(f"usando cache {cache}")
        return json.loads(cache.read_text(encoding="utf-8"))

    print("baixando catalogo FIPE (uma chamada por marca, vai demorar)")
    cat = baixar_catalogo()
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(cat, ensure_ascii=False), encoding="utf-8")
    return cat


def iter_versoes(catalogo):
    """Achata o catalogo em linhas (marca_cod, marca_nome, modelo_cod, modelo_nome)."""
    nome_por_cod = {m["codigo"]: m["nome"] for m in catalogo["marcas"]}
    for marca_cod, versoes in catalogo["modelos"].items():
        marca_nome = nome_por_cod.get(marca_cod, "")
        for v in versoes:
            yield (marca_cod, marca_nome, str(v["codigo"]), v["nome"])
