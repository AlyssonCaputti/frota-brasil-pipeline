"""Peças comuns das linhas de comando."""

import argparse
import logging
import sys
from collections.abc import Callable
from pathlib import Path

from pipeline import arquivos, config
from pipeline.erros import ErroPipeline, FonteNaoEncontrada
from pipeline.mes import Mes

log = logging.getLogger(__name__)


def parser_meses(descricao: str, ultimo: bool = False) -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=descricao)
    grupo = ap.add_mutually_exclusive_group()
    grupo.add_argument("--mes", type=Mes.parse, action="append", help="ex.: abril_2026 (repete)")
    grupo.add_argument("--ano", type=int, help="todo mês do ano disponível")
    if ultimo:
        grupo.add_argument("--ultimo", action="store_true", help="mês mais recente publicado")
    return ap


def mes_padrao() -> Mes:
    return Mes.parse(config.SENATRAN_MES)


def executar(main: Callable[[], int]) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        sys.exit(main())
    except ErroPipeline as e:
        log.error("%s", e)
        sys.exit(1)


def main_conversao(converter: Callable[[Path, Path], int], argv: list[str] | None) -> int:
    """CLI compartilhada pelos dois conversores TXT -> Parquet."""
    ap = parser_meses("Converte o TXT da frota em Parquet")
    ap.add_argument("--apagar-txt", action="store_true", help="apaga o TXT depois de converter")
    args = ap.parse_args(argv)

    if config.USE_SAMPLE:
        raise ErroPipeline("USE_SAMPLE=1: a conversão só roda sobre o dump real em data/raw")

    if args.ano:
        meses = [m for m in Mes.do_ano(args.ano) if arquivos.caminho_txt(m).exists()]
    else:
        meses = args.mes or [mes_padrao()]

    for mes in meses:
        fonte = arquivos.caminho_txt(mes)
        if not fonte.exists():
            raise FonteNaoEncontrada(f"TXT de {mes} não encontrado: {fonte}")
        linhas = converter(fonte, arquivos.caminho_parquet(mes))
        log.info("%s: %s linhas em Parquet", mes, f"{linhas:,}")
        if args.apagar_txt:
            fonte.unlink()
    return 0
