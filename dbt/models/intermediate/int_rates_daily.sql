{{ config(materialized=cl_materialized('table')) }}

{#-
  Daily date spine (history_start_date -> today, US/Eastern) x FRED series, forward-filled.
  Weekly (MORTGAGE30US) and quarterly (TERMCBCCALLNS) series are carried forward to every day; daily
  series are carried over weekends/holidays. Forward fill is done with the portable "count of non-null
  values so far" grouping trick so the SQL is identical on BigQuery and Databricks.
-#}
with numbers as (

    {{ cl_numbers(7000) }}

),

spine as (

    select
        {{ cl_date_shift('day', 'n - 1', "cast('" ~ var('history_start_date') ~ "' as date)") }} as date_day
    from numbers

),

spine_to_today as (

    select date_day
    from spine
    where date_day <= {{ cl_today() }}

),

series as (

    select distinct series_id
    from {{ ref('stg_fred_observations') }}

),

grid as (

    select
        s.series_id,
        d.date_day
    from spine_to_today as d
    cross join series as s

),

observed as (

    select
        g.series_id,
        g.date_day,
        o.value as observed_value
    from grid as g
    left join {{ ref('stg_fred_observations') }} as o
      on o.series_id = g.series_id
     and o.observation_date = g.date_day

),

grouped as (

    select
        series_id,
        date_day,
        observed_value,
        count(observed_value) over (
            partition by series_id
            order by date_day
            rows between unbounded preceding and current row
        ) as fill_group
    from observed

)

select
    series_id,
    date_day,
    observed_value,
    max(observed_value) over (partition by series_id, fill_group) as value,
    observed_value is not null as is_observed
from grouped
