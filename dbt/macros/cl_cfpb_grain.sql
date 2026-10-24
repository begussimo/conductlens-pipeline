{#-
  Which raw CFPB grain this target reads (ARCHITECTURE §3.1 / §3.1b):
    'complaints'  : raw.cfpb_complaints at complaint grain       -> stg_cfpb_complaints
    'daily_cells' : raw.cfpb_complaints_daily + raw.cfpb_codes   -> stg_cfpb_complaints_daily
  BigQuery defaults to daily cells (lifetime write budget) unless CL_BQ_CFPB_GRAIN=complaints.
  Other targets read complaints; the dbt var `cl_cfpb_grain` can force 'daily_cells' for smoke tests.
-#}
{% macro cl_cfpb_grain() -%}
    {%- if target.type == 'bigquery' -%}
        {{ return(env_var('CL_BQ_CFPB_GRAIN', 'daily_cells')) }}
    {%- else -%}
        {{ return(var('cl_cfpb_grain', 'complaints')) }}
    {%- endif -%}
{%- endmacro %}
