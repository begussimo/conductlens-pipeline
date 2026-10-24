{#- Division returning NULL on zero/null denominator in both dialects. -#}
{% macro safe_divide(numerator, denominator) -%}
    {{ return(adapter.dispatch('safe_divide', 'conductlens')(numerator, denominator)) }}
{%- endmacro %}

{% macro default__safe_divide(numerator, denominator) -%}
    case when ({{ denominator }}) = 0 then null else ({{ numerator }}) / ({{ denominator }}) end
{%- endmacro %}

{% macro bigquery__safe_divide(numerator, denominator) -%}
    safe_divide({{ numerator }}, {{ denominator }})
{%- endmacro %}

{% macro databricks__safe_divide(numerator, denominator) -%}
    try_divide({{ numerator }}, {{ denominator }})
{%- endmacro %}

{% macro spark__safe_divide(numerator, denominator) -%}
    case when ({{ denominator }}) = 0 then null else ({{ numerator }}) / ({{ denominator }}) end
{%- endmacro %}
