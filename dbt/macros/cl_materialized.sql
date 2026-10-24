{#-
  Materialisation table (ARCHITECTURE §5):
    folder        Databricks   BigQuery sandbox
    staging       view         view
    intermediate  table        view   (no DML -> no incremental; every table write counts against 10 GiB)
    marts         table        table  (small aggregates)
  Usage in dbt_project.yml / model config: materialized = "{{ cl_materialized('table') }}".
  The folder is inferred from the node's fqn so the macro works from dbt_project.yml.
-#}
{% macro cl_materialized(default) -%}
    {%- set folder = none -%}
    {%- if model is defined and model.fqn is defined and model.fqn | length > 1 -%}
        {%- set folder = model.fqn[1] -%}
    {%- endif -%}
    {%- if target.type == 'bigquery' and folder == 'intermediate' -%}
        view
    {%- elif target.type == 'bigquery' and default == 'incremental' -%}
        table
    {%- else -%}
        {{ default }}
    {%- endif -%}
{%- endmacro %}
