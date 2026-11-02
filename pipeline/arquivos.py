"""Onde ficam os arquivos da frota e como ler o TXT do SENATRAN."""

from __future__ import annotations

import csv
import logging
import shutil
import unicodedata
from collections.abc import Iterator, Sequence
from pathlib import Path

from pipeline import config
from pipeline.erros import LayoutInvalido
from pipeline.mes import Mes

log = logging.getLogger(__name__)

COLUNAS = ("uf", "municipio", "marca_modelo", "ano_fabricacao", "qtd_veiculos")
# acima disso não é sujeira pontual da origem, é layout novo
LIMITE_DESCARTE_PCT = 0.5


def sem_acento(txt: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", txt) if not unicodedata.combining(c))


def caminho_txt(mes: Mes, pasta: Path = config.RAW_DIR) -> Path:
    # o zip extrai com a caixa e o acento do SENATRAN (..._Março_2026.TXT)
    sufixo = f"_{mes}.txt"
    if pasta.is_dir():
        for p in sorted(pasta.iterdir()):
            if sem_acento(p.name.lower()).endswith(sufixo):
                return p
    return pasta / f"i_frota_por_uf_municipio_marca_e_modelo_ano_{mes}.TXT"


def caminho_parquet(mes: Mes, pasta: Path = config.RAW_DIR) -> Path:
    return pasta / f"frota_municipio_{mes}.parquet"


def escolher_fonte(mes: Mes, pasta: Path = config.RAW_DIR) -> Path:
    parquet = caminho_parquet(mes, pasta)
    return parquet if parquet.exists() else caminho_txt(mes, pasta)


def meses_em_disco(ano: int, pasta: Path = config.RAW_DIR) -> list[Mes]:
    return [m for m in Mes.do_ano(ano) if escolher_fonte(m, pasta).exists()]


def substituir(origem: Path, destino: Path) -> None:
    """Troca destino por origem, seja arquivo único ou diretório de part-files."""
    if destino.is_dir():
        shutil.rmtree(destino)
    elif destino.exists():
        destino.unlink()
    origem.replace(destino)


def validar_descarte(
    lidas: int, descartadas: int, origem: str, exemplos: Sequence[str] = ()
) -> None:
    if not descartadas:
        return
    pct = 100 * descartadas / lidas
    log.warning("%s: %d de %d linhas descartadas (%.2f%%)", origem, descartadas, lidas, pct)
    for ex in exemplos:
        log.warning("  %s", ex)
    if pct > LIMITE_DESCARTE_PCT:
        raise LayoutInvalido(
            f"{origem}: {pct:.2f}% das linhas com número de campos errado "
            f"(limite {LIMITE_DESCARTE_PCT}%). Parece layout novo na origem."
        )


class LeituraTxt:
    """Itera o TXT do SENATRAN já com strip, contando as linhas tortas."""

    def __init__(self, caminho: Path):
        self.caminho = caminho
        self.lidas = 0
        self.descartadas = 0
        self.exemplos: list[str] = []

    def __iter__(self) -> Iterator[tuple[str, ...]]:
        self.lidas = self.descartadas = 0
        self.exemplos = []
        with open(self.caminho, encoding="utf-8-sig", newline="") as f:
            leitor = csv.reader(f, delimiter=";")
            next(leitor)  # cabeçalho
            for n, linha in enumerate(leitor, start=2):
                if not linha:
                    continue
                self.lidas += 1
                if len(linha) != len(COLUNAS):
                    self.descartadas += 1
                    if len(self.exemplos) < 5:
                        self.exemplos.append(f"linha {n}: {linha[:6]}")
                    continue
                yield tuple(c.strip() for c in linha)

    def validar(self) -> None:
        validar_descarte(self.lidas, self.descartadas, self.caminho.name, self.exemplos)
