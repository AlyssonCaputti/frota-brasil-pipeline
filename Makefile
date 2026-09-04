# atalhos do dia a dia. `make help` lista tudo.
.PHONY: help up down load year backfill parquet dbt test pipeline clean

DBT_DIR := dbt
export DBT_PROFILES_DIR := $(DBT_DIR)
ANO ?= 2026
# ex: make backfill ANOS="2024 2025"
ANOS ?= 2024 2025 2026

help:  ## lista os targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

up:  ## sobe o postgres
	docker compose up -d

down:  ## derruba o postgres
	docker compose down

load:  ## carrega frota + fipe no schema raw (usa a amostra por padrao)
	python -m pipeline.load_frota
	python -m pipeline.load_fipe

year:  ## baixa e carrega o ano inteiro (ex: make year ANO=2026, precisa USE_SAMPLE=0)
	python -m pipeline.download_senatran --ano $(ANO)
	python -m pipeline.load_frota --ano $(ANO)
	python -m pipeline.load_fipe

backfill:  ## baixa+carrega varios anos em parquet (ex: make backfill ANOS="2024 2025 2026")
	@for ano in $(ANOS); do \
		echo "=== $$ano ==="; \
		python -m pipeline.download_senatran --ano $$ano --parquet || exit 1; \
		python -m pipeline.load_frota --ano $$ano || exit 1; \
	done
	python -m pipeline.load_fipe

parquet:  ## converte os TXT ja baixados de um ano em parquet (ex: make parquet ANO=2026)
	python -m pipeline.to_parquet --ano $(ANO) --apagar-txt

dbt:  ## dbt deps + seed + run
	cd $(DBT_DIR) && dbt deps && dbt seed && dbt run

test:  ## dbt test
	cd $(DBT_DIR) && dbt test

pipeline: up load dbt test  ## roda tudo de ponta a ponta

clean:  ## limpa artefatos do dbt
	cd $(DBT_DIR) && dbt clean
