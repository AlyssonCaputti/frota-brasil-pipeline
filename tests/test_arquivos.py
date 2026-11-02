import pytest
from conftest import linhas_validas

from pipeline import arquivos
from pipeline.erros import LayoutInvalido
from pipeline.mes import Mes


def test_caminho_txt_acha_nome_com_acento_e_caixa(tmp_path):
    real = tmp_path / "I_Frota_por_UF_Municipio_Marca_e_Modelo_Ano_Março_2026.TXT"
    real.touch()
    assert arquivos.caminho_txt(Mes(2026, 3), tmp_path) == real


def test_escolher_fonte_prefere_parquet(tmp_path):
    mes = Mes(2026, 4)
    arquivos.caminho_txt(mes, tmp_path).touch()
    assert arquivos.escolher_fonte(mes, tmp_path).suffix == ".TXT"
    arquivos.caminho_parquet(mes, tmp_path).mkdir()
    assert arquivos.escolher_fonte(mes, tmp_path).suffix == ".parquet"
    assert arquivos.meses_em_disco(2026, tmp_path) == [mes]


def test_leitura_txt_faz_strip_e_conta_tortas(txt_frota):
    caminho = txt_frota([*linhas_validas(3), "SP;SAO PAULO;FIAT/UNO;2010", ""])
    leitura = arquivos.LeituraTxt(caminho)
    linhas = list(leitura)
    assert linhas[0] == ("SP", "SAO PAULO", "FIAT/UNO 1", "2010", "1.0")
    assert (leitura.lidas, leitura.descartadas) == (4, 1)


def test_validar_descarte_aborta_acima_do_limite():
    arquivos.validar_descarte(1000, 5, "ok.TXT")  # 0,5% passa
    with pytest.raises(LayoutInvalido):
        arquivos.validar_descarte(1000, 6, "ruim.TXT")


@pytest.mark.parametrize("destino_e_dir", [True, False])
def test_substituir_troca_arquivo_por_diretorio_e_vice_versa(tmp_path, destino_e_dir):
    destino = tmp_path / "x.parquet"
    origem = tmp_path / "x.parquet.tmp"
    if destino_e_dir:
        destino.mkdir()
        (destino / "part-0.parquet").touch()
        origem.touch()
    else:
        destino.touch()
        origem.mkdir()
    arquivos.substituir(origem, destino)
    assert destino.exists() and not origem.exists()
    assert destino.is_dir() != destino_e_dir
