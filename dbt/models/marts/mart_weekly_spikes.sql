{{ config(materialized=cl_materialized('table')) }}

{#-
  q8: weekly complaint counts per company x issue for the trailing `spike_weeks_kept` weeks with a
  z-score against the 8 complete weeks immediately preceding each week (rows preceding, current week
  excluded). The denominator is greatest(stddev, sqrt(avg), 1), a Poisson floor. Weeks are densified per (company, issue) so a quiet week counts as zero.
  is_spike = z_score > 2 and trailing_8w_avg >= 5.
  Only pairs with at least `spike_min_window_volume` complaints in the window are kept: a pair below that
  can never reach trailing_8w_avg >= 5, and the restriction keeps the table small on the sandbox.
-#}
{% set weeks_kept = var('spike_weeks_kept', 60) %}
{% set min_volume = var('spike_min_window_volume', 40) %}
{% set weeks_loaded = weeks_kept + 8 %}

with current_week as (

    select {{ cl_week_start(cl_today()) }} as current_week_start

),

numbers as (

    {{ cl_numbers(weeks_loaded + 1) }}

),

week_spine as (

    select
        {{ cl_date_shift('day', '-7 * (n.n - 1)', 'cw.current_week_start') }} as received_week
    from numbers as n
    cross join current_week as cw

),

weekly as (

    select
        w.received_week,
        w.company,
        w.issue,
        sum(w.n_complaints) as n_complaints
    from {{ ref('int_complaints_weekly') }} as w
    cross join current_week as cw
    where w.received_week >= {{ cl_date_shift('day', -7 * weeks_loaded, 'cw.current_week_start') }}
      and w.issue is not null
    group by 1, 2, 3

),

pairs as (

    select
        company,
        issue,
        sum(n_complaints) as window_complaints
    from weekly
    group by 1, 2
    having sum(n_complaints) >= {{ min_volume }}

),

dense as (

    select
        s.received_week,
        p.company,
        p.issue,
        coalesce(x.n_complaints, 0) as this_week
    from pairs as p
    cross join week_spine as s
    left join weekly as x
      on x.received_week = s.received_week
     and x.company = p.company
     and x.issue = p.issue

),

windowed as (

    select
        received_week,
        company,
        issue,
        this_week,
        avg(this_week) over (
            partition by company, issue order by received_week
            rows between 8 preceding and 1 preceding
        ) as trailing_8w_avg,
        stddev(this_week) over (
            partition by company, issue order by received_week
            rows between 8 preceding and 1 preceding
        ) as trailing_8w_stddev,
        count(*) over (
            partition by company, issue order by received_week
            rows between 8 preceding and 1 preceding
        ) as n_prior_weeks
    from dense

),

scored as (

    select
        w.received_week,
        w.company,
        w.issue,
        w.this_week,
        case when w.n_prior_weeks = 8 then w.trailing_8w_avg end as trailing_8w_avg,
        case when w.n_prior_weeks = 8 then w.trailing_8w_stddev end as trailing_8w_stddev,
        {#- Counts are roughly Poisson, so the spread of a quiet pair is floored at sqrt(avg) and at 1:
            eight identical weeks (stddev 0) then give a finite, honest z instead of infinity. -#}
        case when w.n_prior_weeks = 8 then
            {{ safe_divide('w.this_week - w.trailing_8w_avg',
                           'greatest(w.trailing_8w_stddev, sqrt(w.trailing_8w_avg), 1.0)') }}
        end as z_score,
        w.received_week = cw.current_week_start as is_current_week,
        w.received_week = {{ cl_date_shift('day', -7, 'cw.current_week_start') }} as is_latest_week
    from windowed as w
    cross join current_week as cw
    where w.received_week > {{ cl_date_shift('day', -7 * weeks_kept, 'cw.current_week_start') }}

)

select
    received_week,
    company,
    issue,
    this_week,
    trailing_8w_avg,
    trailing_8w_stddev,
    z_score,
    coalesce(z_score > 2 and trailing_8w_avg >= 5, false) as is_spike,
    is_current_week,
    is_latest_week
from scored
