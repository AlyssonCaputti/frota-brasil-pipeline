"""Config central do pipeline. Le do .env, com defaults sensatos."""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
SAMPLES_DIR = DATA_DIR / "samples"

SENATRAN_MES = os.getenv("SENATRAN_MES", "abril_2026")
USE_SAMPLE = os.getenv("USE_SAMPLE", "1") == "1"

# CKAN
CKAN_BASE = "https://dados.transportes.gov.br/api/3/action"
CKAN_DATASET_ID = "registro-nacional-de-veiculos-automotores-renavam"

PG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "frota"),
    "user": os.getenv("PGUSER", "frota"),
    "password": os.getenv("PGPASSWORD", "frota"),
}


def pg_dsn():
    return (
        f"host={PG['host']} port={PG['port']} dbname={PG['dbname']} "
        f"user={PG['user']} password={PG['password']}"
    )
