"""Configuração lida do ambiente (.env)."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
SAMPLES_DIR = ROOT / "data" / "samples"
AMOSTRA_FROTA = SAMPLES_DIR / "senatran_frota_sample.csv"
AMOSTRA_FIPE = SAMPLES_DIR / "fipe_catalogo.json"
DBT_DIR = ROOT / "dbt"

SENATRAN_MES = os.getenv("SENATRAN_MES", "abril_2026")
# amostra só quando pedida: esquecer a variável em produção não pode virar dado falso
USE_SAMPLE = os.getenv("USE_SAMPLE", "0") == "1"

CKAN_BASE = "https://dados.transportes.gov.br/api/3/action"
CKAN_DATASET_ID = "registro-nacional-de-veiculos-automotores-renavam"


def pg_dsn() -> str:
    return " ".join(
        f"{chave}={os.getenv(var, padrao)}"
        for chave, var, padrao in (
            ("host", "PGHOST", "localhost"),
            ("port", "PGPORT", "5432"),
            ("dbname", "PGDATABASE", "frota"),
            ("user", "PGUSER", "frota"),
            ("password", "PGPASSWORD", "frota"),
        )
    )
