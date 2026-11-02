import csv
import io

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import linhas_validas

from pipeline import load_frota
from pipeline.erros import LayoutInvalido
from pipeline.mes import Mes

ABRIL = Mes(2026, 4)


def test_lotes_parquet_le_diretorio_de_part_files(tmp_path):
    pasta = tmp_path / "frota.parquet"
    pasta.mkdir()
    for parte in range(2):
        tabela = pa.table({c: [f"{c}{parte}"] for c in load_frota.COLUNAS})
        pq.write_table(tabela, pasta / f"part-{parte}.parquet")

    lotes = list(load_frota._lotes_parquet(pasta, "abril_2026"))
    linhas = [r for buf, _ in lotes for r in csv.reader(io.TextIOWrapper(buf), delimiter="\t")]
    assert sum(n for _, n in lotes) == 2
    assert all(r[-1] == "abril_2026" and len(r) == 6 for r in linhas)


def _contar(conn, mes):
    with conn.cursor() as cur:
        cur.execute("select count(*) from raw.frota_municipio where mes_referencia = %s", (mes,))
        return cur.fetchone()[0]


def test_carga_idempotente_e_rollback_em_mes_vazio(postgres, txt_frota):
    mes = Mes(1999, 1)  # mês que nenhum dado real usa
    assert load_frota.carregar_mes(postgres, mes, txt_frota(linhas_validas(5))) == 5
    assert load_frota.carregar_mes(postgres, mes, txt_frota(linhas_validas(5))) == 5
    assert _contar(postgres, str(mes)) == 5

    with pytest.raises(LayoutInvalido):
        load_frota.carregar_mes(postgres, mes, txt_frota([], nome="vazio.TXT"))
    assert _contar(postgres, str(mes)) == 5  # o mês antigo continua lá

    with postgres, postgres.cursor() as cur:
        cur.execute("delete from raw.frota_municipio where mes_referencia = %s", (str(mes),))
