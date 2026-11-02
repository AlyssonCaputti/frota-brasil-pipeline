import pyarrow.dataset as ds
import pytest
from conftest import linhas_validas, spark_disponivel

from pipeline import to_parquet
from pipeline.erros import LayoutInvalido


def conversores():
    params = [pytest.param(to_parquet.converter, id="pyarrow")]
    if spark_disponivel():
        from pipeline import to_parquet_spark

        params.append(pytest.param(to_parquet_spark.converter, id="spark"))
    return params


def ler(caminho):
    return ds.dataset(caminho, format="parquet").to_table().to_pylist()


@pytest.mark.parametrize("converter", conversores())
def test_converte_com_strip_e_tolera_pouca_linha_torta(converter, txt_frota, tmp_path):
    fonte = txt_frota([*linhas_validas(300), "SP;SAO PAULO;FIAT/UNO;2010"])
    destino = tmp_path / "frota.parquet"

    assert converter(fonte, destino) == 300
    linhas = sorted(ler(destino), key=lambda r: int(r["marca_modelo"].split()[-1]))
    assert linhas[0] == {
        "uf": "SP",
        "municipio": "SAO PAULO",
        "marca_modelo": "FIAT/UNO 1",
        "ano_fabricacao": "2010",
        "qtd_veiculos": "1.0",
    }


@pytest.mark.parametrize("converter", conversores())
def test_aborta_com_muita_linha_torta_e_nao_deixa_lixo(converter, txt_frota, tmp_path):
    fonte = txt_frota([*linhas_validas(10), "a;b", "a;b;c;d;e;f", "a;b;c"])
    destino = tmp_path / "frota.parquet"
    destino.write_bytes(b"parquet bom de antes")

    with pytest.raises(LayoutInvalido):
        converter(fonte, destino)
    assert destino.read_bytes() == b"parquet bom de antes"
    assert not (tmp_path / "frota.parquet.tmp").exists()


def test_main_recusa_amostra(monkeypatch):
    from pipeline import config
    from pipeline.erros import ErroPipeline

    monkeypatch.setattr(config, "USE_SAMPLE", True)
    with pytest.raises(ErroPipeline):
        to_parquet.main(["--ano", "2026", "--apagar-txt"])
