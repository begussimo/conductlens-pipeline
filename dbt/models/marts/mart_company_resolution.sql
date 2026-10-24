{{ config(materialized=cl_materialized('table')) }}

{#-
  q4: TTM response mix per company x response_category, with the industry share of the same category.
  Industry shares use every company; company rows are limited to companies with at least
  `company_min_complaints_ttm` (100) TTM complaints to keep the sandbox table small.
-#}
{% set min_ttm = var('company_min_complaints_ttm', 100) %}

with ttm as (

    select
        company,
        response_category,
        n_complaints
    from {{ ref('int_complaints_daily') }}
    where received_date >= {{ cl_ttm_start() }}

),

eligible_companies as (

    select company
    from {{ ref('dim_company') }}
    where complaints_ttm >= {{ min_ttm }}

),

by_company as (

    select
        t.company,
        t.response_category,
        sum(t.n_complaints) as complaints
    from ttm as t
    join eligible_companies as e
      on e.company = t.company
    group by 1, 2

),

industry as (

    select
        response_category,
        sum(n_complaints) as industry_complaints,
        sum(sum(n_complaints)) over () as industry_total
    from ttm
    group by 1

)

select
    c.company,
    c.response_category,
    c.complaints,
    sum(c.complaints) over (partition by c.company) as company_complaints_ttm,
    {{ safe_divide('c.complaints', 'sum(c.complaints) over (partition by c.company)') }} as share_of_company,
    i.industry_complaints,
    {{ safe_divide('i.industry_complaints', 'i.industry_total') }} as industry_share
from by_company as c
left join industry as i
  on i.response_category = c.response_category
