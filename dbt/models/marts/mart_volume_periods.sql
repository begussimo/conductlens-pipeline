{{ config(materialized=cl_materialized('table')) }}

{#-
  q1: industry WTD / MTD / YTD complaint counts vs the same period one year earlier.
  "Today" is the US/Eastern calendar date (cl_today()); period start = Monday of this week / first of the
  month / January 1st; the comparison window is the same start and end dates shifted back one year.
-#}
with today as (

    select {{ cl_today() }} as today_date

),

periods as (

    select
        'week' as period_type,
        {{ cl_week_start('today_date') }} as period_start,
        today_date as period_end
    from today
    union all
    select
        'month' as period_type,
        {{ cl_month_start('today_date') }} as period_start,
        today_date as period_end
    from today
    union all
    select
        'ytd' as period_type,
        {{ cl_year_start('today_date') }} as period_start,
        today_date as period_end
    from today

),

windows as (

    select
        period_type,
        period_start,
        period_end,
        {{ cl_date_shift('year', -1, 'period_start') }} as ly_period_start,
        {{ cl_date_shift('year', -1, 'period_end') }} as ly_period_end
    from periods

),

complaints as (

    select received_date, n_complaints
    from {{ ref('int_complaints_daily') }}
    where received_date >= {{ cl_date_shift('day', -400, cl_today()) }}

),

counted as (

    select
        w.period_type,
        w.period_start,
        w.period_end,
        w.ly_period_start,
        w.ly_period_end,
        sum(case when c.received_date between w.period_start and w.period_end then c.n_complaints else 0 end) as complaints_current,
        sum(case when c.received_date between w.ly_period_start and w.ly_period_end then c.n_complaints else 0 end) as complaints_last_year
    from windows as w
    left join complaints as c
      on c.received_date between w.ly_period_start and w.period_end
    group by 1, 2, 3, 4, 5

)

select
    'industry' as scope,
    period_type,
    period_start,
    period_end,
    {{ dbt.datediff('period_start', 'period_end', 'day') }} + 1 as days_in_period,
    complaints_current,
    ly_period_start,
    ly_period_end,
    complaints_last_year,
    {{ safe_divide('complaints_current - complaints_last_year', 'complaints_last_year') }} as pct_change
from counted
