"""Conexão com o Postgres."""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import IO, Any

import psycopg2
from psycopg2.extensions import connection, cursor

from pipeline import config

DDL = config.ROOT / "pipeline" / "sql" / "raw_ddl.sql"


@contextmanager
def conectar() -> Iterator[connection]:
    conn = psycopg2.connect(config.pg_dsn())
    try:
        yield conn
    finally:
        conn.close()


def aplicar_ddl(conn: connection) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(DDL.read_text(encoding="utf-8"))


def escalar(cur: cursor, sql: str, params: tuple = ()) -> Any:
    cur.execute(sql, params)
    linha = cur.fetchone()
    return linha[0] if linha else None


def copiar(cur: cursor, tabela: str, colunas: Sequence[str], dados: IO) -> None:
    cur.copy_expert(
        f"COPY {tabela} ({', '.join(colunas)}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')",
        dados,
    )
