{#- Size bucket from total deposits in USD (ARCHITECTURE §5 int_fdic_deposits_by_holding). -#}
{% macro cl_size_bucket(dep_usd) -%}
    case
        when {{ dep_usd }} is null then null
        when {{ dep_usd }} < 1000000000 then '<1B'
        when {{ dep_usd }} < 10000000000 then '1-10B'
        when {{ dep_usd }} < 50000000000 then '10-50B'
        when {{ dep_usd }} < 250000000000 then '50-250B'
        else '>250B'
    end
{%- endmacro %}

{#- The product groups that are offered by chartered banks (used for the match-rate honesty test). -#}
{% macro cl_bank_product_groups() -%}
    ('mortgage', 'credit_card', 'checking_savings', 'auto_loan', 'personal_loan', 'student_loan', 'prepaid')
{%- endmacro %}

{#- Trailing-twelve-month window start: today minus 365 days (inclusive). -#}
{% macro cl_ttm_start() -%}
    {{ cl_date_shift('day', -365, cl_today()) }}
{%- endmacro %}

{#- Databricks-only Delta liquid clustering; no-op elsewhere. -#}
{% macro cl_liquid_cluster(columns) -%}
    {%- if target.type == 'databricks' -%}
        {{ return(columns) }}
    {%- else -%}
        {{ return(none) }}
    {%- endif -%}
{%- endmacro %}
