{#-
  Integer series 1..upper_bound as column `n`, built from cross-joined two-row subqueries.
  Used instead of dbt_utils.generate_series because that macro yields a FLOAT (power(2, i)), which both
  BigQuery `INTERVAL n DAY` and Databricks `timestampadd` reject, and it wraps its own WITH clause.
-#}
{% macro cl_numbers(upper_bound) -%}
    {%- set bits = namespace(value=1) -%}
    {%- for i in range(1, 40) -%}
        {%- if upper_bound <= 2 ** i -%}
            {%- set bits.value = i -%}
            {%- break -%}
        {%- endif -%}
    {%- endfor -%}
    select cast(
        {% for i in range(bits.value) -%}
            p{{ i }}.b * {{ 2 ** i }}{% if not loop.last %} + {% endif %}
        {%- endfor %} + 1
        as {{ dbt.type_int() }}) as n
    from
        {% for i in range(bits.value) -%}
            (select 0 as b union all select 1 as b) as p{{ i }}{% if not loop.last %} cross join {% endif %}
        {%- endfor %}
    where
        {% for i in range(bits.value) -%}
            p{{ i }}.b * {{ 2 ** i }}{% if not loop.last %} + {% endif %}
        {%- endfor %} + 1 <= {{ upper_bound }}
{%- endmacro %}
