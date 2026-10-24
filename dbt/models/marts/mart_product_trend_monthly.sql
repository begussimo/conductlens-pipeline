{{ config(materialized=cl_materialized('table')) }}

{#- q2: month x product_group counts, share of the month, and year-over-year growth. -#}
with monthly as (

    select
        received_month,
        product_group,
        sum(n_complaints) as complaints
    from {{ ref('int_complaints_daily') }}
    group by 1, 2

),

with_totals as (

    select
        received_month,
        product_group,
        complaints,
        sum(complaints) over (partition by received_month) as industry_complaints
    from monthly

)

select
    cur.received_month,
    cur.product_group,
    cur.complaints,
    cur.industry_complaints,
    {{ safe_divide('cur.complaints', 'cur.industry_complaints') }} as share_of_month,
    prior.complaints as complaints_prior_year,
    {{ safe_divide('cur.complaints - prior.complaints', 'prior.complaints') }} as yoy_growth,
    cur.received_month < {{ cl_month_start(cl_today()) }} as is_complete_month
from with_totals as cur
left join with_totals as prior
  on prior.product_group = cur.product_group
 and prior.received_month = {{ cl_date_shift('year', -1, 'cur.received_month') }}
