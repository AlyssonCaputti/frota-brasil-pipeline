import os
import shutil
import sys
from pathlib import Path

import pytest

CABECALHO = "UF;Município;Marca Modelo;Ano Fabricação Veículo CRV;Qtd. Veículos\n"


@pytest.fixture
def txt_frota(tmp_path: Path):
    """Cria um TXT no formato do SENATRAN (utf-8 com BOM, `;`)."""

    def criar(linhas: list[str], nome: str = "frota.TXT") -> Path:
        caminho = tmp_path / nome
        caminho.write_text(CABECALHO + "\n".join(linhas) + "\n", encoding="utf-8-sig")
        return caminho

    return criar


def linhas_validas(n: int) -> list[str]:
    return [f"SP;SAO PAULO ;FIAT/UNO {i};2010; {i}.0" for i in range(1, n + 1)]


def spark_disponivel() -> bool:
    try:
        import pyspark  # noqa: F401
    except ImportError:
        return False
    if not shutil.which("java"):
        return False
    return not (sys.platform == "win32" and not os.getenv("HADOOP_HOME"))


@pytest.fixture
def postgres():
    """Conexão real; os testes pulam se não houver Postgres em PG*."""
    from pipeline import db

    try:
        with db.conectar() as conn:
            db.aplicar_ddl(conn)
            yield conn
    except Exception as e:
        if "could not connect" in str(e) or "Connection refused" in str(e):
            pytest.skip("sem Postgres")
        raise
