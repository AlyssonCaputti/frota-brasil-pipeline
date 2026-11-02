import pytest

from pipeline.mes import Mes


def test_parse_e_str_ida_e_volta():
    assert str(Mes.parse("Marco_2026")) == "marco_2026"
    assert Mes.parse("abril_2026") == Mes(2026, 4)


def test_ordem_cronologica():
    meses = [Mes.parse(m) for m in ("dezembro_2025", "janeiro_2026", "marco_2025")]
    assert [str(m) for m in sorted(meses)] == ["marco_2025", "dezembro_2025", "janeiro_2026"]


@pytest.mark.parametrize("ruim", ["abril2026", "april_2026", "abril_26", ""])
def test_parse_recusa_formato_errado(ruim):
    with pytest.raises(ValueError):
        Mes.parse(ruim)


def test_do_ano():
    assert len(Mes.do_ano(2026)) == 12
