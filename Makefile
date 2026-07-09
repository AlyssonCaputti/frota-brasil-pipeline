# atalhos do dia a dia. `make help` lista tudo.
.PHONY: help up down load dbt test pipeline clean

DBT_DIR := dbt
export DBT_PROFILES_DIR := $(DBT_DIR)

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

dbt:  ## dbt deps + seed + run
	cd $(DBT_DIR) && dbt deps && dbt seed && dbt run

test:  ## dbt test
	cd $(DBT_DIR) && dbt test

pipeline: up load dbt test  ## roda tudo de ponta a ponta

clean:  ## limpa artefatos do dbt
	cd $(DBT_DIR) && dbt clean
