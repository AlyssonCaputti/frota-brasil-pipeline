"""Baixa o dump de frota por município do SENATRAN (portal CKAN).

A URL muda todo mês, então resolvo pelo package_show do CKAN, que lista todos
os meses publicados numa chamada só. Mês que já está em disco é pulado.

    python -m pipeline.download_senatran                   # SENATRAN_MES do .env
    python -m pipeline.download_senatran --ultimo          # mais recente publicado
    python -m pipeline.download_senatran --ano 2026 --parquet
"""

import logging
import re
import time
import zipfile
from pathlib import Path

import requests

from pipeline import arquivos, cli, config
from pipeline.erros import FonteNaoEncontrada, LayoutInvalido
from pipeline.mes import NOMES, Mes

log = logging.getLogger(__name__)

PADRAO_URL = re.compile(rf"_municipio_marca_e_modelo_ano_(({'|'.join(NOMES)})_\d{{4}})\.zip$")
# o portal começa a recusar depois de vários arquivos grandes seguidos
PAUSA_ENTRE_MESES = 20
TENTATIVAS = 6


def meses_publicados() -> dict[Mes, str]:
    r = requests.get(
        f"{config.CKAN_BASE}/package_show", params={"id": config.CKAN_DATASET_ID}, timeout=60
    )
    r.raise_for_status()
    achados = {}
    for recurso in r.json()["result"]["resources"]:
        if m := PADRAO_URL.search(recurso["url"].lower()):
            achados[Mes.parse(m[1])] = recurso["url"]
    return dict(sorted(achados.items()))


def ultimo_publicado(publicados: dict[Mes, str] | None = None) -> Mes:
    publicados = meses_publicados() if publicados is None else publicados
    if not publicados:
        raise FonteNaoEncontrada("o CKAN não listou nenhum dump de município")
    return max(publicados)


def _stream(url: str, destino: Path, offset: int) -> None:
    cabecalhos = {"Range": f"bytes={offset}-"} if offset else {}
    with requests.get(url, stream=True, timeout=600, headers=cabecalhos) as r:
        r.raise_for_status()
        # 206 = retomando; 200 = o servidor ignorou o Range e manda tudo de novo
        modo = "ab" if r.status_code == 206 else "wb"
        with open(destino, modo) as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)


def baixar(url: str, destino: Path) -> None:
    """Baixa com retry. Da 2a tentativa em diante retoma de onde parou (Range)."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    log.info("baixando %s", url)
    for tentativa in range(TENTATIVAS):
        # a 1a sempre do zero: um parcial antigo pode ser de outra versão do arquivo
        offset = destino.stat().st_size if tentativa and destino.exists() else 0
        try:
            _stream(url, destino, offset)
            return
        except requests.RequestException as e:
            if tentativa == TENTATIVAS - 1:
                raise
            espera = 15 * 2**tentativa
            log.warning("%s; retomando em %ds", type(e).__name__, espera)
            time.sleep(espera)


def descompactar(zip_path: Path) -> Path:
    with zipfile.ZipFile(zip_path) as z:
        txts = [n for n in z.namelist() if n.lower().endswith(".txt")]
        if not txts:
            # antes de 2020 o portal publica Access (.mdb/.accdb) e .rar
            raise LayoutInvalido(
                f"{zip_path.name} não tem TXT (dumps anteriores a 2020 não são lidos)"
            )
        z.extract(txts[0], zip_path.parent)
    return zip_path.parent / txts[0]


def obter_mes(mes: Mes, url: str, parquet: bool) -> bool:
    """Garante o mês em disco. Devolve True se precisou baixar."""
    if arquivos.caminho_parquet(mes).exists():
        log.info("%s: parquet já existe", mes)
        return False

    txt = arquivos.caminho_txt(mes)
    baixou = not txt.exists()
    if baixou:
        zip_path = config.RAW_DIR / f"frota_municipio_{mes}.zip"
        baixar(url, zip_path)
        txt = descompactar(zip_path)
        zip_path.unlink()

    if parquet:
        # import tardio: quem só baixa não precisa de pyspark/JVM
        from pipeline import to_parquet_spark

        to_parquet_spark.converter(txt, arquivos.caminho_parquet(mes))
        txt.unlink()
    return baixou


def main(argv: list[str] | None = None) -> int:
    ap = cli.parser_meses("Baixa o dump de frota do SENATRAN", ultimo=True)
    ap.add_argument("--parquet", action="store_true", help="converte e apaga o TXT")
    args = ap.parse_args(argv)

    if config.USE_SAMPLE:
        log.info("USE_SAMPLE=1: nada a baixar, a amostra está em data/samples")
        return 0

    publicados = meses_publicados()
    if args.ultimo:
        meses = [ultimo_publicado(publicados)]
    elif args.ano:
        meses = [m for m in publicados if m.ano == args.ano]
    else:
        meses = args.mes or [cli.mes_padrao()]

    faltando = [m for m in meses if m not in publicados]
    if faltando or not meses:
        disponiveis = ", ".join(map(str, publicados))
        raise FonteNaoEncontrada(
            f"não publicado: {faltando or args.ano}. Disponíveis: {disponiveis}"
        )

    baixou_antes = False
    for mes in meses:
        if baixou_antes:
            time.sleep(PAUSA_ENTRE_MESES)
        baixou_antes = obter_mes(mes, publicados[mes], args.parquet)
    return 0


if __name__ == "__main__":
    cli.executar(main)
