{#-
  Schema naming per ARCHITECTURE §2: `<prefix>_<folder schema>` verbatim, never `<target schema>_<custom>`.
  Prefix: CL_BQ_DATASET_PREFIX on BigQuery, CL_DBX_SCHEMA_PREFIX on Databricks (default `conductlens`).
  Models without a custom schema (e.g. packages) land in the profile's default schema.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ cl_schema_prefix() }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{% macro cl_schema_prefix() -%}
    {%- if target.type == 'bigquery' -%}
        {{ env_var('CL_BQ_DATASET_PREFIX', 'conductlens') }}
    {%- else -%}
        {{ env_var('CL_DBX_SCHEMA_PREFIX', 'conductlens') }}
    {%- endif -%}
{%- endmacro %}
