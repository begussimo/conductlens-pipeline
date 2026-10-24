{{ config(
    materialized=cl_materialized('table'),
    liquid_clustered_by=cl_liquid_cluster(['received_date', 'company'])
) }}

{#-
  THE one model every mart reads (ARCHITECTURE §5). Grain:
    received_date, company, product, sub_product, issue, state, company_response, timely_response, consumer_disputed
  with `n_complaints`, enriched with product_group, FDIC mapping, digital-first flag, response_category,
  is_timely and calendar helpers (received_week = Monday). Identical columns on both warehouses:
    Databricks : group by over complaint-grain stg_cfpb_complaints (Delta table, liquid-clustered)
    BigQuery   : pass-through of stg_cfpb_complaints_daily (view; sandbox write budget)
-#}
with cells as (

{% if cl_cfpb_grain() == 'daily_cells' %}

    select
        received_date,
        received_month_id,
        company,
        company_name_norm,
        product,
        sub_product,
        issue,
        state,
        company_response,
        response_category,
        timely_response,
        is_timely,
        consumer_disputed,
        is_disputed,
        n_complaints
    from {{ ref('stg_cfpb_complaints_daily') }}

{% else %}

    select
        received_date,
        received_month_id,
        company,
        max(company_name_norm) as company_name_norm,
        product,
        sub_product,
        issue,
        state,
        company_response,
        {{ cl_response_category('company_response') }} as response_category,
        timely_response,
        {{ cl_yes_no_bool('timely_response') }} as is_timely,
        consumer_disputed,
        {{ cl_yes_no_bool('consumer_disputed') }} as is_disputed,
        count(*) as n_complaints
    from {{ ref('stg_cfpb_complaints') }}
    group by
        received_date, received_month_id, company, product, sub_product, issue, state,
        company_response, timely_response, consumer_disputed

{% endif %}

)

select
    c.received_date,
    c.received_month_id,
    {{ cl_week_start('c.received_date') }} as received_week,
    {{ cl_month_start('c.received_date') }} as received_month,
    extract(year from c.received_date) as received_year,
    c.company,
    c.company_name_norm,
    m.holding_key,
    coalesce(m.match_method, 'unmatched') as match_method,
    coalesce(m.is_digital_first, false) as is_digital_first,
    m.digital_segment,
    c.product,
    c.sub_product,
    coalesce(pg.product_group, 'other') as product_group,
    c.issue,
    c.state,
    c.company_response,
    c.response_category,
    c.timely_response,
    c.is_timely,
    c.consumer_disputed,
    c.is_disputed,
    c.n_complaints
from cells as c
left join {{ ref('int_product_groups') }} as pg
  on pg.product = c.product
 and coalesce(pg.sub_product, '') = coalesce(c.sub_product, '')
left join {{ ref('int_company_mapping') }} as m
  on m.company = c.company
