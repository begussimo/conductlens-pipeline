#!/usr/bin/env bash
# Bootstrap the Databricks Free Edition side of ConductLens.
#   * lists SQL warehouses and prints the HTTP path of the first one
#   * creates schemas <prefix>_raw/_staging/_marts/_snapshots in CL_DBX_CATALOG
#   * creates the UC volume <prefix>_raw.<CL_DBX_VOLUME> (landing area for COPY/MERGE)
#   * prints the .env lines to fill (never the secret values)
# Requires the Databricks CLI (`databricks`) authenticated via DATABRICKS_CONFIG_PROFILE,
# or DATABRICKS_HOST + DATABRICKS_TOKEN in the environment. Idempotent: re-run freely.
set -euo pipefail

if [[ -f .env ]]; then set -a; . ./.env; set +a; fi

command -v databricks >/dev/null || {
  echo "databricks CLI not found. Install: https://docs.databricks.com/dev-tools/cli/install.html" >&2
  exit 1
}
command -v jq >/dev/null || { echo "jq is required (brew install jq)" >&2; exit 1; }

PROFILE_ARGS=()
if [[ -n "${DATABRICKS_CONFIG_PROFILE:-}" ]]; then
  PROFILE_ARGS=(--profile "$DATABRICKS_CONFIG_PROFILE")
elif [[ -z "${DATABRICKS_TOKEN:-}" ]]; then
  PROFILE_ARGS=(--profile DEFAULT)
fi
dbx() { databricks "${PROFILE_ARGS[@]}" "$@"; }

CATALOG="${CL_DBX_CATALOG:-workspace}"
PREFIX="${CL_DBX_SCHEMA_PREFIX:-conductlens}"
VOLUME="${CL_DBX_VOLUME:-landing}"

echo "== Workspace"
dbx current-user me -o json | jq -r '"user: \(.userName)"'

echo
echo "== SQL warehouses"
WAREHOUSES_JSON="$(dbx warehouses list -o json)"
echo "$WAREHOUSES_JSON" | jq -r '.[] | "\(.name)\t\(.state)\t\(.odbc_params.path // "")"' | column -t -s $'\t'
HTTP_PATH="$(echo "$WAREHOUSES_JSON" | jq -r '.[0].odbc_params.path // empty')"
if [[ -z "$HTTP_PATH" ]]; then
  echo "No SQL warehouse found. Create one in the UI (SQL Warehouses -> Create) and re-run." >&2
  exit 1
fi

echo
echo "== Schemas in catalog '$CATALOG'"
for suffix in raw staging marts snapshots; do
  schema="${PREFIX}_${suffix}"
  if out="$(dbx schemas create "$schema" "$CATALOG" 2>&1)"; then
    echo "created  $CATALOG.$schema"
  elif grep -qi "already exists" <<<"$out"; then
    echo "exists   $CATALOG.$schema"
  else
    echo "$out" >&2; exit 1
  fi
done

echo
echo "== Volume"
if out="$(dbx volumes create "$CATALOG" "${PREFIX}_raw" "$VOLUME" MANAGED 2>&1)"; then
  echo "created  $CATALOG.${PREFIX}_raw.$VOLUME"
elif grep -qi "already exists" <<<"$out"; then
  echo "exists   $CATALOG.${PREFIX}_raw.$VOLUME"
else
  echo "$out" >&2; exit 1
fi

echo
echo "== .env lines (copy into .env and airflow/.env; fill DATABRICKS_TOKEN yourself, it is never printed)"
HOST="$(dbx auth env 2>/dev/null | sed -n 's/.*"DATABRICKS_HOST": *"\([^"]*\)".*/\1/p' | head -1)"
echo "DATABRICKS_HOST=${HOST:-https://<your-workspace>.cloud.databricks.com}"
echo "DATABRICKS_TOKEN=            # personal access token: User Settings -> Developer -> Access tokens"
echo "DATABRICKS_HTTP_PATH=${HTTP_PATH}"
echo "CL_DBX_CATALOG=${CATALOG}"
echo "CL_DBX_SCHEMA_PREFIX=${PREFIX}"
echo "CL_DBX_VOLUME=${VOLUME}"
echo
echo "Raw tables are created by: make ensure-tables"
