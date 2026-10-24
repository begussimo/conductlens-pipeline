{{ config(materialized=cl_materialized('table')) }}

{#-
  q3: month x company complaints and timely share. `is_timely` is the CFPB's own 15-calendar-day flag
  (var timely_window_days); the response date is not published so the flag is not recomputed.
  `ttm_timely_share` is computed over the trailing 12 calendar months via a range self-join (exact
  even when a company has months without complaints). Output keeps the trailing
  `company_monthly_history_months` (36) months and only companies with at least
  `company_min_complaints_ttm` (100) complaints in the trailing twelve months, to stay small on the sandbox.
-#}
{% set months_kept = var('company_monthly_history_months', 36) %}
{% set min_ttm = var('company_min_complaints_ttm', 100) %}

with monthly as (

    select
        received_month,
        company,
        sum(n_complaints) as complaints,
        sum(case when is_timely then n_complaints else 0 end) as timely_complaints,
        sum(case when is_timely is not null then n_complaints else 0 end) as timely_known
    from {{ ref('int_complaints_daily') }}
    where received_month >= {{ cl_month_start(cl_date_shift('month', -(months_kept + 12), cl_today())) }}
    group by 1, 2

),

eligible_companies as (

    select company
    from {{ ref('dim_company') }}
    where complaints_ttm >= {{ min_ttm }}

),

kept as (

    select m.*
    from monthly as m
    join eligible_companies as e
      on e.company = m.company
    where m.received_month >= {{ cl_month_start(cl_date_shift('month', -months_kept, cl_today())) }}

),

ttm as (

    select
        k.received_month,
        k.company,
        sum(m.complaints) as ttm_complaints,
        sum(m.timely_complaints) as ttm_timely_complaints,
        sum(m.timely_known) as ttm_timely_known
    from kept as k
    join monthly as m
      on m.company = k.company
     and m.received_month <= k.received_month
     and m.received_month > {{ cl_date_shift('month', -12, 'k.received_month') }}
    group by 1, 2

)

select
    k.received_month,
    k.company,
    k.complaints,
    k.timely_complaints,
    k.timely_known,
    {{ safe_divide('k.timely_complaints', 'k.timely_known') }} as timely_share,
    t.ttm_complaints,
    {{ safe_divide('t.ttm_timely_complaints', 't.ttm_timely_known') }} as ttm_timely_share,
    {{ var('timely_window_days') }} as timely_window_days,
    k.received_month < {{ cl_month_start(cl_today()) }} as is_complete_month
from kept as k
left join ttm as t
  on t.company = k.company
 and t.received_month = k.received_month
