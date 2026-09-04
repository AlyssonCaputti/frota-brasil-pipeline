"""Backfill de varios anos sem estourar o disco.

O problema: raw.frota_municipio guarda uf/municipio/marca_modelo como texto
repetido em cada linha ("SAO PAULO" 22M vezes). 31 meses viram ~700M linhas
e passam de 60 GB - mais do que a maioria das maquinas tem sobrando.

A saida: o raw existe so pro dbt ler uma vez. Como os marts sao incrementais
por mes e staging/intermediate sao views, eu processo um mes por vez:

    load raw (1 mes) -> dbt run (absorve o mes) -> apaga o mes do raw

O pico de disco fica no tamanho de UM mes em vez de 31, os marts acumulam o
historico inteiro e o Parquet segue como copia fria pra reprocessar.

Uso:
    python -m pipeline.backfill --anos 2024 2025 2026
    python -m pipeline.backfill --anos 2026 --manter-raw   # nao limpa o raw
"""

import argparse
import subprocess
import sys

import psycopg2

from pipeline import config, load_frota, to_parquet


def _dbt(*args):
    """Roda dbt no diretorio do projeto, com o profiles.yml de lá."""
    dbt_dir = config.ROOT / "dbt"
    proc = subprocess.run(
        ["dbt", *args],
        cwd=dbt_dir,
        env={"DBT_PROFILES_DIR": str(dbt_dir), **__import__("os").environ},
    )
    if proc.returncode != 0:
        raise RuntimeError(f"dbt {' '.join(args)} falhou (exit {proc.returncode})")


def meses_disponiveis(anos):
    """Meses com Parquet ou TXT em data/raw, em ordem cronologica."""
    fora = []
    for ano in anos:
        for mes in config.MESES:
            chave = f"{mes}_{ano}"
            if (
                to_parquet.caminho_parquet(chave).exists()
                or config.caminho_frota(chave).exists()
            ):
                fora.append(chave)
    return fora


def limpar_raw(conn, mes):
    """Esvazia o raw. Os marts ja absorveram e as views nao guardam nada.

    TRUNCATE em vez de DELETE: delete marca as linhas como mortas mas nao
    devolve o disco sem VACUUM, e 3.7 GB por mes vezes 31 nao caberia. Como
    processo um mes por vez, o raw so tem esse mes - truncar a tabela toda
    e seguro e libera na hora.
    """
    with conn.cursor() as cur:
        cur.execute("select count(*) from raw.frota_municipio")
        n = cur.fetchone()[0]
        cur.execute("truncate table raw.frota_municipio")
    conn.commit()
    return n


def tamanho_raw(conn):
    with conn.cursor() as cur:
        cur.execute("select pg_total_relation_size('raw.frota_municipio')")
        return cur.fetchone()[0]


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--anos", nargs="+", required=True, help="ex: --anos 2024 2025")
    ap.add_argument(
        "--manter-raw",
        action="store_true",
        help="nao apaga o mes do raw depois do dbt (precisa de MUITO disco)",
    )
    args = ap.parse_args(argv)

    if config.USE_SAMPLE:
        raise SystemExit("USE_SAMPLE=1 nao combina com backfill. seta USE_SAMPLE=0.")

    meses = meses_disponiveis(args.anos)
    if not meses:
        raise SystemExit(
            f"nenhum mes de {' '.join(args.anos)} em {config.RAW_DIR}. "
            "roda o download_senatran --ano <ano> --parquet primeiro."
        )

    print(f"{len(meses)} mes(es) pra processar: {', '.join(meses)}\n")

    # DDL antes de tudo: o load_fipe faz truncate, e a tabela precisa existir
    conn_ddl = psycopg2.connect(config.pg_dsn())
    with conn_ddl.cursor() as cur:
        load_frota.aplicar_ddl(cur)
    conn_ddl.commit()
    conn_ddl.close()

    # o fipe nao tem mes, entao carrega uma vez e serve pra todos
    print("=== carregando catalogo FIPE ===")
    from pipeline import load_fipe

    load_fipe.main()

    # deps + seed uma vez: nao mudam entre meses
    print("\n=== dbt deps + seed ===")
    _dbt("deps")
    _dbt("seed")

    # as views de staging/intermediate precisam existir antes dos marts
    # referenciarem. sao views, entao criar custa quase nada.
    print("\n=== dbt run: views de staging/intermediate ===")
    _dbt("run", "--select", "staging intermediate")

    conn = psycopg2.connect(config.pg_dsn())
    conn.autocommit = False
    processados = []
    try:
        for i, mes in enumerate(meses, start=1):
            print(f"\n{'=' * 60}\n[{i}/{len(meses)}] {mes}\n{'=' * 60}")

            with conn.cursor() as cur:
                load_frota.aplicar_ddl(cur)
                total, descartadas = load_frota.carregar_mes(cur, mes)
            conn.commit()
            print(f"\n  raw: {total:,} linhas"
                  + (f" ({descartadas:,} descartadas)" if descartadas else ""))

            # so os marts: staging/intermediate sao views, nao custam nada
            _dbt("run", "--select", "marts")

            if args.manter_raw:
                print(f"  --manter-raw: {mes} fica no raw")
            else:
                apagadas = limpar_raw(conn, mes)
                print(f"  raw truncado: {apagadas:,} linhas liberadas "
                      f"(marts ja tem o mes; parquet segue como copia fria)")

            processados.append(mes)
            print(f"  raw agora: {tamanho_raw(conn) / 1e9:.2f} GB")

        print(f"\n{'=' * 60}\n=== dbt test ===\n{'=' * 60}")
        _dbt("test")
    except Exception:
        conn.rollback()
        print(f"\nabortou em {len(processados)}/{len(meses)} meses. "
              f"processados: {', '.join(processados) or '(nenhum)'}")
        raise
    finally:
        conn.close()

    print(f"\nok: {len(processados)} mes(es) nos marts")
    return 0


if __name__ == "__main__":
    sys.exit(main())
