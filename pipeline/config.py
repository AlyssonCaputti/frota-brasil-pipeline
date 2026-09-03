"""Config central do pipeline. Le do .env, com defaults sensatos."""
import os
import unicodedata
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
SAMPLES_DIR = DATA_DIR / "samples"

SENATRAN_MES = os.getenv("SENATRAN_MES", "abril_2026")
USE_SAMPLE = os.getenv("USE_SAMPLE", "1") == "1"

# nomes de mes do jeito que o SENATRAN escreve no arquivo (sem acento). a
# ordem e o que permite ordenar mes_referencia cronologicamente.
MESES = (
    "janeiro",
    "fevereiro",
    "marco",
    "abril",
    "maio",
    "junho",
    "julho",
    "agosto",
    "setembro",
    "outubro",
    "novembro",
    "dezembro",
)

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


def _sem_acento(txt: str) -> str:
    """Equivalente python do unaccent que o norm_txt() faz no SQL."""
    return "".join(
        c for c in unicodedata.normalize("NFKD", txt) if not unicodedata.combining(c)
    )


def caminho_frota(mes: str) -> Path:
    """TXT da frota do mes (ou a amostra, se USE_SAMPLE=1)."""
    if USE_SAMPLE:
        return SAMPLES_DIR / "senatran_frota_sample.csv"
    # a chave do mes vem da url, sem acento (marco_2026), mas o zip extrai o
    # TXT com a caixa e o acento do SENATRAN (I_Frota_..._Março_2026.TXT).
    # entao normaliza os dois lados: casar o nome na letra deixaria marco de
    # fora do --ano sem reclamar, e no linux (airflow, CI) nem acharia.
    sufixo = _sem_acento(f"_{mes.lower()}.txt")
    if RAW_DIR.is_dir():
        for p in sorted(RAW_DIR.iterdir()):
            if _sem_acento(p.name.lower()).endswith(sufixo):
                return p
    return RAW_DIR / f"i_frota_por_uf_municipio_marca_e_modelo_ano_{mes}.TXT"
