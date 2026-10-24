{#-
  Normalise a company name for fuzzy-free joining between CFPB `company` text and FDIC `name`/`namehcr`:
    1. upper-case
    2. '&' -> ' AND '
    3. strip everything that is not A-Z, 0-9 or space
    4. collapse runs of spaces
    5. repeatedly strip trailing legal tokens (INC, LLC, L L C, N A, NATIONAL ASSOCIATION, HOLDINGS, ...)
    6. trim
  Written as nested regexp_replace calls: RE2 (BigQuery) and Java regex (Databricks) both support
  these simple patterns (no lookbehind, no \b). Each legal-token pass is applied several times so that
  e.g. "FOO BANCORP INC" -> "FOO BANCORP" -> "FOO".
-#}
{% macro normalize_company_name(col) -%}
    {%- set legal_tokens = [
        'INCORPORATED', 'INC', 'L L C', 'LLC', 'L P', 'LP', 'LLP',
        'CORPORATION', 'CORP', 'COMPANY', 'CO',
        'NATIONAL ASSOCIATION', 'N A', 'NA',
        'HOLDINGS', 'HOLDING', 'GROUP', 'BANCORPORATION', 'BANCORP', 'FINANCIAL',
        'FSB', 'SERVICES', 'TECHNOLOGIES', 'LTD', 'LIMITED', 'PLC', 'AB', 'SA', 'NV',
        'THE', 'AND'
    ] -%}
    {#- one pass = strip any single trailing legal token (and a trailing "THE" prefix) -#}
    {%- set token_pattern = '( (' ~ legal_tokens | join('|') ~ '))+$' -%}
    {#- namespace: a plain `set` inside a for-loop would not survive the loop in Jinja -#}
    {%- set ns = namespace(expr='upper(cast(' ~ col ~ ' as ' ~ dbt.type_string() ~ '))') -%}
    {%- set ns.expr = cl_regexp_replace_all(ns.expr, '&', ' AND ') -%}
    {%- set ns.expr = cl_regexp_replace_all(ns.expr, '[^A-Z0-9 ]', ' ') -%}
    {%- set ns.expr = cl_regexp_replace_all(ns.expr, ' +', ' ') -%}
    {%- set ns.expr = "trim(" ~ ns.expr ~ ")" -%}
    {%- for _ in range(3) -%}
        {%- set ns.expr = cl_regexp_replace_all(ns.expr, token_pattern, '') -%}
    {%- endfor -%}
    {%- set ns.expr = cl_regexp_replace_all(ns.expr, '^THE ', '') -%}
    {%- set ns.expr = "nullif(trim(" ~ ns.expr ~ "), '')" -%}
    {{ ns.expr }}
{%- endmacro %}


{#-
  regexp_replace that replaces EVERY match. BigQuery REGEXP_REPLACE and Databricks regexp_replace already
  do; DuckDB (used only for the offline smoke test) needs the 'g' option.
-#}
{% macro cl_regexp_replace_all(expr, pattern, replacement) -%}
    {{ return(adapter.dispatch('cl_regexp_replace_all', 'conductlens')(expr, pattern, replacement)) }}
{%- endmacro %}

{% macro default__cl_regexp_replace_all(expr, pattern, replacement) -%}
    {{ return("regexp_replace(" ~ expr ~ ", '" ~ pattern ~ "', '" ~ replacement ~ "')") }}
{%- endmacro %}

{% macro duckdb__cl_regexp_replace_all(expr, pattern, replacement) -%}
    {{ return("regexp_replace(" ~ expr ~ ", '" ~ pattern ~ "', '" ~ replacement ~ "', 'g')") }}
{%- endmacro %}
