{#-
  Week starting Monday as a DATE (ARCHITECTURE §8: weeks start Monday).
  BigQuery DATE_TRUNC(..., WEEK) defaults to Sunday, so the Monday variant is spelled out;
  Databricks date_trunc('week') is already Monday-based but returns a TIMESTAMP.
-#}
{% macro cl_week_start(col) -%}
    {{ return(adapter.dispatch('cl_week_start', 'conductlens')(col)) }}
{%- endmacro %}

{% macro default__cl_week_start(col) -%}
    cast({{ dbt.date_trunc('week', col) }} as date)
{%- endmacro %}

{% macro bigquery__cl_week_start(col) -%}
    date_trunc(cast({{ col }} as date), week(monday))
{%- endmacro %}

{% macro databricks__cl_week_start(col) -%}
    cast(date_trunc('week', {{ col }}) as date)
{%- endmacro %}

{% macro spark__cl_week_start(col) -%}
    cast(date_trunc('week', {{ col }}) as date)
{%- endmacro %}

{#- First day of the month as a DATE. -#}
{% macro cl_month_start(col) -%}
    cast({{ dbt.date_trunc('month', col) }} as date)
{%- endmacro %}

{#- First day of the year as a DATE. -#}
{% macro cl_year_start(col) -%}
    cast({{ dbt.date_trunc('year', col) }} as date)
{%- endmacro %}

{#- Shift a date by whole days/years as DATE (dbt.dateadd returns TIMESTAMP on some adapters). -#}
{% macro cl_date_shift(datepart, interval, col) -%}
    cast({{ dbt.dateadd(datepart, interval, col) }} as date)
{%- endmacro %}

{#- Natural log; `ln` exists in both dialects. -#}
{% macro cl_ln(x) -%}
    ln({{ x }})
{%- endmacro %}
