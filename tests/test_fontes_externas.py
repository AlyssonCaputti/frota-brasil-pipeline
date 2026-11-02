import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest
import requests

from pipeline import dimensiona_bq, download_senatran, fipe_client
from pipeline.mes import Mes


class Resposta:
    def __init__(self, status=200, dados=None):
        self.status_code = status
        self._dados = dados

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def json(self):
        return self._dados


def test_ultimo_publicado_ordena_por_data(monkeypatch):
    base = "https://x/i_frota_por_uf_municipio_marca_e_modelo_ano_{}.zip"
    recursos = [{"url": base.format(m)} for m in ("dezembro_2025", "julho_2026", "marco_2026")]
    recursos.append({"url": "https://x/frota_por_tipo_julho_2026.xlsx"})
    monkeypatch.setattr(
        download_senatran.requests,
        "get",
        lambda *a, **k: Resposta(dados={"result": {"resources": recursos}}),
    )
    assert download_senatran.ultimo_publicado() == Mes(2026, 7)


def test_fipe_tenta_de_novo_em_5xx_e_timeout(monkeypatch):
    respostas = iter([Resposta(503), requests.Timeout(), Resposta(200, ["ok"])])

    def get(*a, **k):
        r = next(respostas)
        if isinstance(r, Exception):
            raise r
        return r

    monkeypatch.setattr(fipe_client.requests, "get", get)
    monkeypatch.setattr(fipe_client.time, "sleep", lambda s: None)
    assert fipe_client._get("https://fipe") == ["ok"]


def _parquet_em_partes(pasta, marcas):
    pasta.mkdir()
    for i, marca in enumerate(marcas):
        tabela = pa.table(
            {
                "uf": ["SP"],
                "municipio": ["SAO PAULO"],
                "marca_modelo": [marca],
                "ano_fabricacao": ["2010"],
                "qtd_veiculos": ["1.0"],
            }
        )
        pq.write_table(tabela, pasta / f"part-{i}.parquet")
    return ds.dataset(pasta, format="parquet")


def test_dimensiona_bq_le_diretorio_do_spark(tmp_path):
    dataset = _parquet_em_partes(tmp_path / "frota.parquet", ["FIAT/UNO", "VW/GOL"])
    assert dimensiona_bq.bytes_por_coluna(dataset)["marca_modelo"] == (2 + 8) + (2 + 6)
    assert {m for m, _ in dimensiona_bq.fatia_por_marca(dataset)} == {"VW", "FIAT"}
    assert dimensiona_bq.tamanho_em_disco(dataset) > 0


def test_load_bq_apaga_o_mes_e_sobe_em_lotes(tmp_path, monkeypatch):
    load_bq = pytest.importorskip("pipeline.load_bq")
    monkeypatch.setattr(load_bq, "LOTE", 2)
    fonte = tmp_path / "frota.parquet"
    pq.write_table(pa.table({c: ["x"] * 5 for c in load_bq.COLUNAS}), fonte)

    class Job:
        def result(self):
            return None

    class Cliente:
        def __init__(self):
            self.queries, self.lotes = [], []

        def query(self, sql, job_config):
            self.queries.append(sql)
            return Job()

        def load_table_from_file(self, buf, tabela, job_config):
            self.lotes.append(pq.read_table(buf).column("mes_referencia").to_pylist())
            return Job()

    cliente = Cliente()
    assert load_bq.carregar_mes(cliente, "p.raw.frota", Mes(2026, 4), fonte) == 5
    assert cliente.queries[0].startswith("delete")
    assert [len(lote) for lote in cliente.lotes] == [2, 2, 1]
    assert {m for lote in cliente.lotes for m in lote} == {"abril_2026"}
