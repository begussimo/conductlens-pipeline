{{ config(
    enabled=(cl_cfpb_grain() == 'daily_cells'),
    materialized=cl_materialized('view')
) }}

{#-
  BigQuery only (ARCHITECTURE §3.1b). Current version of every daily cell of the CFPB log:
  newest `_loaded_at` per (received_date + 8 code columns), tombstones removed, codes decoded through
  `cfpb_codes` (code 0 -> null). Derived columns (`is_timely`, `response_category`, `company_name_norm`)
  follow stg_cfpb_complaints exactly, at cell grain, with `n_complaints` carried along.
-#}
with versions as (

    select
        *,
        row_number() over (
            partition by
                received_date, company_code, product_code, sub_product_code, issue_code,
                state_code, company_response_code, timely_response_code, consumer_disputed_code
            order by _loaded_at desc
        ) as rn
    from {{ source('conductlens_raw', 'cfpb_complaints_daily') }}

),

current_cells as (

    select * from versions
    where rn = 1
      and not coalesce(_is_tombstone, false)
      and n_complaints is not null

),

codes as (

    select dimension, code, value
    from (
        select
            dimension,
            code,
            value,
            row_number() over (partition by dimension, code order by _loaded_at desc) as rn
        from {{ source('conductlens_raw', 'cfpb_codes') }}
    ) as c
    where rn = 1

),

decoded as (

    select
        d.received_date,
        d.received_month_id,
        {% for dim in ['company', 'product', 'sub_product', 'issue', 'state', 'company_response', 'timely_response', 'consumer_disputed'] -%}
        case when d.{{ dim }}_code = 0 then null else c_{{ dim }}.value end as {{ dim }},
        {% endfor -%}
        d.n_complaints,
        d._source,
        d._source_as_of,
        d._loaded_at
    from current_cells as d
    {% for dim in ['company', 'product', 'sub_product', 'issue', 'state', 'company_response', 'timely_response', 'consumer_disputed'] -%}
    left join codes as c_{{ dim }}
      on c_{{ dim }}.dimension = '{{ dim }}'
     and c_{{ dim }}.code = d.{{ dim }}_code
    {% endfor %}

)

select
    received_date,
    received_month_id,
    product,
    sub_product,
    issue,
    company,
    {{ normalize_company_name('company') }} as company_name_norm,
    state,
    company_response,
    {{ cl_response_category('company_response') }} as response_category,
    timely_response,
    {{ cl_yes_no_bool('timely_response') }} as is_timely,
    consumer_disputed,
    {{ cl_yes_no_bool('consumer_disputed') }} as is_disputed,
    n_complaints,
    _source,
    _source_as_of,
    _loaded_at
from decoded
