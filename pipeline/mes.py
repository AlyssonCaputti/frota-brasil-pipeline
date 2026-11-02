"""Mês de referência no formato que o SENATRAN usa nos arquivos: "abril_2026"."""

from __future__ import annotations

import re
from dataclasses import dataclass

NOMES = (
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
_PADRAO = re.compile(rf"^({'|'.join(NOMES)})_(\d{{4}})$")


@dataclass(frozen=True, order=True)
class Mes:
    ano: int
    numero: int

    @classmethod
    def parse(cls, chave: str) -> Mes:
        m = _PADRAO.match(chave.strip().lower())
        if not m:
            raise ValueError(f"mês inválido: {chave!r} (ex.: abril_2026)")
        return cls(int(m[2]), NOMES.index(m[1]) + 1)

    @classmethod
    def do_ano(cls, ano: int) -> list[Mes]:
        return [cls(ano, n) for n in range(1, 13)]

    @property
    def nome(self) -> str:
        return NOMES[self.numero - 1]

    def __str__(self) -> str:
        return f"{self.nome}_{self.ano}"
