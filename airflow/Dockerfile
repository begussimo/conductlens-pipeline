# ConductLens Airflow image. Build context = REPO ROOT (see docker-compose.yml).
#
# Two Python environments live in the image:
#   /home/airflow/.local      Airflow 2.10.5 + astronomer-cosmos (DAG parsing, operators)
#   /opt/airflow/dbt_venv     dbt-core + dbt-bigquery + dbt-databricks + the ingestion
#                             package `conductlens_ingest` (console script `cl-ingest`)
# Keeping dbt out of the Airflow env avoids dependency clashes; Cosmos only needs the
# path of the dbt binary (ExecutionConfig.dbt_executable_path). The ingestion package
# declares dbt as a dependency in pyproject.toml, so it lives in the same venv and the
# DAGs call it as /opt/airflow/dbt_venv/bin/cl-ingest (CL_INGEST_BIN).
FROM apache/airflow:2.10.5-python3.12

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.12
ARG CONSTRAINTS=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends git curl \
 && apt-get clean && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /opt/airflow/dbt_venv /opt/airflow/repo /opt/airflow/secrets \
 && chown -R airflow:root /opt/airflow/dbt_venv /opt/airflow/repo /opt/airflow/secrets
USER airflow

# 1) Airflow env: Cosmos (and nothing dbt-related).
COPY --chown=airflow:root airflow/requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt --constraint "${CONSTRAINTS}"

# 2) dbt venv: dbt adapters + the ingestion package (editable, so the runtime bind
#    mount ../ingestion -> /opt/airflow/repo/ingestion is picked up without rebuilds).
COPY --chown=airflow:root airflow/requirements-dbt.txt /opt/airflow/requirements-dbt.txt
COPY --chown=airflow:root pyproject.toml /opt/airflow/repo/pyproject.toml
COPY --chown=airflow:root ingestion /opt/airflow/repo/ingestion
RUN python -m venv /opt/airflow/dbt_venv \
 && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir -r /opt/airflow/requirements-dbt.txt \
 && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir -e /opt/airflow/repo \
 && /opt/airflow/dbt_venv/bin/dbt --version \
 && /opt/airflow/dbt_venv/bin/cl-ingest --help >/dev/null

ENV CL_DBT_EXECUTABLE=/opt/airflow/dbt_venv/bin/dbt \
    CL_INGEST_BIN=/opt/airflow/dbt_venv/bin/cl-ingest \
    CL_DBT_PROJECT_DIR=/opt/airflow/dbt \
    PATH="/opt/airflow/dbt_venv/bin:${PATH}"
