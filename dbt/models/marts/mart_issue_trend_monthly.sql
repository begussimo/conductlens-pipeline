{{ config(materialized=cl_materialized('table')) }}

{#-
  q2: month x product_group x issue counts with trailing-twelve-month (TTM) and prior-TTM sums so the
  fastest-growing issues can be ranked. Months are densified per (product_group, issue) so the 12-row
  windows are exact calendar windows. Output keeps the trailing `company_monthly_history_months` (36)
  months (24 more are read so prior-TTM is exact at the oldest kept month) to stay small on the sandbox.
-#}
{% set months_kept = var('company_monthly_history_months', 36) %}

with monthly as (

    select
        received_month,
        product_group,
        issue,
        sum(n_complaints) as complaints
    from {{ ref('int_complaints_daily') }}
    where issue is not null
      and received_month >= {{ cl_month_start(cl_date_shift('month', -(months_kept + 24), cl_today())) }}
    group by 1, 2, 3

),

months as (

    select distinct received_month from monthly

),

pairs as (

    select
        product_group,
        issue,
        min(received_month) as first_month
    from monthly
    group by 1, 2

),

grid as (

    select
        m.received_month,
        p.product_group,
        p.issue
    from pairs as p
    cross join months as m
    where m.received_month >= p.first_month

),

dense as (

    select
        g.received_month,
        g.product_group,
        g.issue,
        coalesce(x.complaints, 0) as complaints
    from grid as g
    left join monthly as x
      on x.received_month = g.received_month
     and x.product_group = g.product_group
     and x.issue = g.issue

),

windowed as (

    select
        received_month,
        product_group,
        issue,
        complaints,
        sum(complaints) over (
            partition by product_group, issue order by received_month
            rows between 11 preceding and current row
        ) as ttm_complaints,
        sum(complaints) over (
            partition by product_group, issue order by received_month
            rows between 23 preceding and 12 preceding
        ) as prior_ttm_complaints,
        count(*) over (
            partition by product_group, issue order by received_month
            rows between 23 preceding and current row
        ) as n_months_available
    from dense

)

select
    received_month,
    product_group,
    issue,
    complaints,
    ttm_complaints,
    case when n_months_available >= 24 then prior_ttm_complaints end as prior_ttm_complaints,
    case when n_months_available >= 24 then ttm_complaints - prior_ttm_complaints end as ttm_abs_change,
    case when n_months_available >= 24 then {{ safe_divide('ttm_complaints - prior_ttm_complaints', 'prior_ttm_complaints') }} end as ttm_growth,
    received_month < {{ cl_month_start(cl_today()) }} as is_complete_month,
    received_month = {{ cl_month_start(cl_date_shift('month', -1, cl_today())) }} as is_latest_complete_month
from windowed
where received_month >= {{ cl_month_start(cl_date_shift('month', -months_kept, cl_today())) }}
