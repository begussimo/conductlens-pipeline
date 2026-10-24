{#-
  Today's calendar date in US/Eastern (CFPB dates are Eastern calendar dates, ARCHITECTURE §8).
  A run at 13:00 UTC and one at 23:30 UTC agree on "today".
-#}
{% macro cl_today() -%}
    {{ return(adapter.dispatch('cl_today', 'conductlens')()) }}
{%- endmacro %}

{% macro default__cl_today() -%}
    current_date
{%- endmacro %}

{% macro bigquery__cl_today() -%}
    current_date('America/New_York')
{%- endmacro %}

{% macro databricks__cl_today() -%}
    to_date(from_utc_timestamp(current_timestamp(), 'America/New_York'))
{%- endmacro %}

{% macro spark__cl_today() -%}
    to_date(from_utc_timestamp(current_timestamp(), 'America/New_York'))
{%- endmacro %}
