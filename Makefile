# ConductLens developer entry points. Every recipe sources ./.env so the CLI, dbt and
# docker compose all see the same variables. Executables are taken from .venv explicitly.
SHELL := /bin/bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

VENV    := .venv
PY      := $(VENV)/bin/python
INGEST  := $(VENV)/bin/cl-ingest
DBT     := $(VENV)/bin/dbt
DBTDIRS := --profiles-dir $(CURDIR)/dbt --project-dir $(CURDIR)/dbt   # dbt wants these after the subcommand
COMPOSE := docker compose -f airflow/docker-compose.yml --env-file airflow/.env
# Sources .env; if DATABRICKS_TOKEN is unset, dbt gets it from the Databricks CLI profile in
# ~/.databrickscfg (DATABRICKS_CONFIG_PROFILE, default DEFAULT). The value is exported to the
# recipe's shell only and never printed.
LOADENV  = set -a; [ -f .env ] && . ./.env; set +a; \
  if [ -z "$${DATABRICKS_TOKEN:-}" ] && [ -f "$$HOME/.databrickscfg" ]; then \
    export DATABRICKS_TOKEN="$$(awk -v p="[$${DATABRICKS_CONFIG_PROFILE:-DEFAULT}]" -F' *= *' \
      '$$0==p{f=1;next} /^\[/{f=0} f&&$$1=="token"{print $$2;exit}' "$$HOME/.databrickscfg")"; fi
START   ?=
END     ?=
SOURCES ?= cfpb,fred

.PHONY: help setup ensure-tables ingest-cfpb ingest-all backfill dbt-dbx dbt-bq dbt-docs \
        test lint airflow-up airflow-down airflow-logs airflow-dag-test evidence parity

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-16s %s\n", $$1, $$2}'

setup: ## create .venv, install the package (+dev) and dbt packages
	uv venv $(VENV) --python 3.12
	uv pip install --python $(PY) -e ".[dev]"
	[ -f .env ] || cp .env.example .env
	[ -f airflow/.env ] || cp airflow/.env.example airflow/.env
	$(LOADENV); $(DBT) deps $(DBTDIRS)
	@echo "done. Edit .env and airflow/.env, then: make ensure-tables"

ensure-tables: ## create datasets/schemas/volume/raw tables on all CL_SINKS
	$(LOADENV); $(INGEST) ensure-tables

ingest-cfpb: ## incremental CFPB load
	$(LOADENV); $(INGEST) run --source cfpb --mode incremental

ingest-all: ## incremental load of cfpb, fdic, fred
	$(LOADENV); $(INGEST) run --source all --mode incremental

backfill: ## historical load: make backfill START=2024-01-01 END=2024-06-30 [SOURCES=cfpb,fred]
	@[ -n "$(START)" ] && [ -n "$(END)" ] || { echo "usage: make backfill START=YYYY-MM-DD END=YYYY-MM-DD [SOURCES=cfpb,fred]"; exit 2; }
	$(LOADENV)
	for s in $$(echo "$(SOURCES)" | tr ',' ' '); do
	  $(INGEST) run --source "$$s" --mode backfill --start "$(START)" --end "$(END)"
	done

dbt-dbx: ## dbt build on Databricks (staging, intermediate, marts, snapshots, tests)
	$(LOADENV); $(DBT) build --target dbx $(DBTDIRS)

dbt-bq: ## dbt build on BigQuery (snapshot disabled in-project)
	$(LOADENV); $(DBT) build --target bq $(DBTDIRS)

dbt-docs: ## generate and serve dbt docs for the default target
	$(LOADENV); $(DBT) docs generate $(DBTDIRS) && $(DBT) docs serve $(DBTDIRS)

test: ## pytest + dbt parse on both targets
	$(LOADENV)
	$(VENV)/bin/pytest -q
	$(DBT) parse --target dbx $(DBTDIRS) && $(DBT) parse --target bq $(DBTDIRS)

lint: ## ruff + sqlfluff
	$(LOADENV)
	$(VENV)/bin/ruff check ingestion airflow/dags
	$(VENV)/bin/ruff format --check ingestion airflow/dags
	cd dbt && ../$(VENV)/bin/sqlfluff lint models snapshots

airflow-up: ## build the image and start Postgres + Airflow (UI on http://localhost:8080)
	@[ -f airflow/.env ] || { echo "airflow/.env missing: cp airflow/.env.example airflow/.env and fill it"; exit 2; }
	mkdir -p airflow/logs data evidence secrets
	$(COMPOSE) up -d --build

airflow-down: ## stop the stack (metadata DB volume is kept; add -v by hand to wipe it)
	$(COMPOSE) down

airflow-logs: ## tail scheduler + webserver logs
	$(COMPOSE) logs -f --tail=200 airflow-scheduler airflow-webserver

airflow-dag-test: ## import-check the DAG files inside the running scheduler
	$(COMPOSE) exec airflow-scheduler airflow dags list-import-errors
	$(COMPOSE) exec airflow-scheduler airflow dags list

evidence: ## write evidence/airflow_runs_<date>.md and refresh evidence/scheduled_runs.md
	$(LOADENV)
	scripts/make_evidence.sh
	$(INGEST) evidence

parity: ## compare marts across both warehouses (exit non-zero on >0.1% drift)
	$(LOADENV); $(INGEST) parity-check
