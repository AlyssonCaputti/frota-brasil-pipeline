# Atalhos do dia a dia. `make help` lista tudo.
.PHONY: help up down load dbt test pipeline year backfill parquet check clean

# absoluto: as receitas fazem `cd dbt` e um caminho relativo apontaria para dbt/dbt
export DBT_PROFILES_DIR := $(CURDIR)/dbt
ANO ?= 2026
ANOS ?= 2024 2025 2026

help:  ## lista os targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

up:  ## sobe o postgres e espera ficar saudável
	docker compose up -d --wait

down:  ## derruba o postgres
	docker compose down

load:  ## carrega frota + FIPE no schema raw
	python -m pipeline.load_frota
	python -m pipeline.load_fipe

dbt:  ## dbt deps + seed + run
	cd dbt && dbt deps && dbt seed && dbt run

test:  ## dbt test (dados + unitários)
	cd dbt && dbt test

pipeline: up load dbt test  ## tudo de ponta a ponta

year:  ## baixa e carrega um ano (ex.: make year ANO=2026)
	python -m pipeline.download_senatran --ano $(ANO) --parquet
	python -m pipeline.load_frota --ano $(ANO)
	python -m pipeline.load_fipe

backfill:  ## vários anos, um mês por vez no raw (ex.: make backfill ANOS="2024 2025")
	@for ano in $(ANOS); do python -m pipeline.download_senatran --ano $$ano --parquet || exit 1; done
	python -m pipeline.backfill --anos $(ANOS)

parquet:  ## converte os TXT de um ano em Parquet (ex.: make parquet ANO=2026)
	python -m pipeline.to_parquet_spark --ano $(ANO) --apagar-txt

check:  ## lint, tipos e testes python
	ruff check . && ruff format --check . && mypy pipeline && pytest

clean:  ## limpa artefatos do dbt
	cd dbt && dbt clean
