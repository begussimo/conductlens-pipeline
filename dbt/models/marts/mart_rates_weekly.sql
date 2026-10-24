{{ config(materialized=cl_materialized('table')) }}

{#- q9 chart series: week (Monday) x FRED series weekly average rate, plus weekly mortgage / credit-card complaint counts. -#}
with rates as (

    select
        {{ cl_week_start('date_day') }} as received_week,
        series_id,
        avg(value) as avg_rate,
        min(value) as min_rate,
        max(value) as max_rate
    from {{ ref('int_rates_daily') }}
    where value is not null
    group by 1, 2

),

complaints as (

    select
        received_week,
        sum(case when product_group = 'mortgage' then n_complaints else 0 end) as mortgage_complaints,
        sum(case when product_group = 'credit_card' then n_complaints else 0 end) as credit_card_complaints,
        sum(n_complaints) as total_complaints
    from {{ ref('int_complaints_weekly') }}
    group by 1

)

select
    r.received_week,
    r.series_id,
    r.avg_rate,
    r.min_rate,
    r.max_rate,
    coalesce(c.mortgage_complaints, 0) as mortgage_complaints,
    coalesce(c.credit_card_complaints, 0) as credit_card_complaints,
    coalesce(c.total_complaints, 0) as total_complaints,
    r.received_week < {{ cl_week_start(cl_today()) }} as is_complete_week
from rates as r
left join complaints as c
  on c.received_week = r.received_week
