{{ config(materialized=cl_materialized('table')) }}

{#-
  q9: do rate moves lead complaint volumes? For product_group in (mortgage, credit_card) and each FRED
  series, weekly complaints since rates_corr_start_date are correlated with the weekly average rate
  lagged by 0..max_lag_weeks weeks:
    corr_levels  = corr(complaints_w, rate_{w-lag})
    corr_changes = corr(ln(complaints_w) - ln(complaints_{w-4}), rate_{w-lag} - rate_{w-lag-4})
  is_best_lag marks the lag with the largest |corr_changes| per (product_group, series_id).
  The lag lives here, not in Hex.
-#}
{% set max_lag = var('max_lag_weeks', 26) %}

with week_spine as (

    -- every Monday covered by the daily rate spine since the correlation start date
    select distinct {{ cl_week_start('date_day') }} as received_week
    from {{ ref('int_rates_daily') }}
    where date_day >= cast('{{ var("rates_corr_start_date") }}' as date)

),

complete_weeks as (

    select received_week
    from week_spine
    where received_week < {{ cl_week_start(cl_today()) }}

),

product_group_list as (

    select 'mortgage' as product_group
    union all
    select 'credit_card' as product_group

),

weekly_counts as (

    select
        received_week,
        product_group,
        sum(n_complaints) as complaints
    from {{ ref('int_complaints_weekly') }}
    where product_group in ('mortgage', 'credit_card')
    group by 1, 2

),

complaints_dense as (

    select
        w.received_week,
        g.product_group,
        coalesce(c.complaints, 0) as complaints
    from complete_weeks as w
    cross join product_group_list as g
    left join weekly_counts as c
      on c.received_week = w.received_week
     and c.product_group = g.product_group

),

complaints_changes as (

    select
        received_week,
        product_group,
        complaints,
        lag(complaints, 4) over (partition by product_group order by received_week) as complaints_4w_ago
    from complaints_dense

),

complaints_final as (

    select
        received_week,
        product_group,
        complaints,
        case
            when complaints > 0 and complaints_4w_ago > 0
            then {{ cl_ln('complaints') }} - {{ cl_ln('complaints_4w_ago') }}
        end as complaints_log_change_4w
    from complaints_changes

),

rates_weekly as (

    select
        {{ cl_week_start('date_day') }} as received_week,
        series_id,
        avg(value) as avg_rate
    from {{ ref('int_rates_daily') }}
    where value is not null
    group by 1, 2

),

rates_changes as (

    select
        received_week,
        series_id,
        avg_rate,
        avg_rate - lag(avg_rate, 4) over (partition by series_id order by received_week) as rate_change_4w
    from rates_weekly

),

lags as (

    select n - 1 as lag_weeks
    from ({{ cl_numbers(max_lag + 1) }}) as s

),

paired as (

    select
        c.product_group,
        r.series_id,
        l.lag_weeks,
        c.complaints,
        c.complaints_log_change_4w,
        r.avg_rate,
        r.rate_change_4w
    from complaints_final as c
    cross join lags as l
    join rates_changes as r
      on r.received_week = {{ cl_date_shift('day', '-7 * l.lag_weeks', 'c.received_week') }}

),

correlated as (

    select
        product_group,
        series_id,
        lag_weeks,
        count(*) as n_weeks,
        corr(complaints, avg_rate) as corr_levels,
        corr(complaints_log_change_4w, rate_change_4w) as corr_changes
    from paired
    group by 1, 2, 3

)

select
    product_group,
    series_id,
    lag_weeks,
    n_weeks,
    corr_levels,
    corr_changes,
    coalesce(
        abs(corr_changes) = max(abs(corr_changes)) over (partition by product_group, series_id),
        false
    ) as is_best_lag
from correlated
