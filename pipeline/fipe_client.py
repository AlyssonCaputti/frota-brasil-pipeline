"""Catálogo FIPE de carros (API pública da parallelum), com cache em disco.

Formato do catálogo:
    {"marcas":  [{"codigo": "1", "nome": "Acura"}, ...],
     "modelos": {"1": [{"codigo": 1, "nome": "Integra GS 1.8"}, ...]}}
"""

import json
import logging
import time
from collections.abc import Iterator
from typing import Any

import requests

from pipeline import config

log = logging.getLogger(__name__)

API = "https://parallelum.com.br/fipe/api/v1/carros"
CACHE = config.RAW_DIR / "fipe_catalogo.json"
PAUSA = 0.4  # entre chamadas, para não tomar 429
TENTATIVAS = 5


def _get(url: str) -> Any:
    for tentativa in range(TENTATIVAS):
        try:
            r = requests.get(url, timeout=30)
            if r.status_code != 429 and r.status_code < 500:
                r.raise_for_status()
                return r.json()
            motivo = f"HTTP {r.status_code}"
        except (requests.Timeout, requests.ConnectionError) as e:
            motivo = type(e).__name__
        espera = 2**tentativa
        log.warning("%s em %s, nova tentativa em %ds", motivo, url, espera)
        time.sleep(espera)
    raise requests.exceptions.RetryError(f"desisti de {url} depois de {TENTATIVAS} tentativas")


def baixar_catalogo() -> dict:
    marcas = _get(f"{API}/marcas")
    modelos = {}
    for marca in marcas:
        dados = _get(f"{API}/marcas/{marca['codigo']}/modelos")
        modelos[marca["codigo"]] = dados.get("modelos", dados)
        time.sleep(PAUSA)
    return {"marcas": marcas, "modelos": modelos}


def carregar_catalogo(atualizar: bool = False) -> dict:
    if config.USE_SAMPLE:
        return json.loads(config.AMOSTRA_FIPE.read_text(encoding="utf-8"))

    if CACHE.exists() and not atualizar:
        return json.loads(CACHE.read_text(encoding="utf-8"))

    log.info("baixando catálogo FIPE (uma chamada por marca)")
    try:
        catalogo = baixar_catalogo()
    except requests.RequestException:
        if not CACHE.exists():
            raise
        # API fora do ar não pode travar a carga do mês: sigo com o catálogo anterior
        log.warning("FIPE indisponível, usando o cache de %s", CACHE)
        return json.loads(CACHE.read_text(encoding="utf-8"))

    CACHE.parent.mkdir(parents=True, exist_ok=True)
    CACHE.write_text(json.dumps(catalogo, ensure_ascii=False), encoding="utf-8")
    return catalogo


def iter_versoes(catalogo: dict) -> Iterator[tuple[str, str, str, str]]:
    """(marca_codigo, marca_nome, modelo_codigo, modelo_nome) para cada versão."""
    nome_da_marca = {m["codigo"]: m["nome"] for m in catalogo["marcas"]}
    for codigo, versoes in catalogo["modelos"].items():
        for v in versoes:
            yield codigo, nome_da_marca.get(codigo, ""), str(v["codigo"]), v["nome"]
