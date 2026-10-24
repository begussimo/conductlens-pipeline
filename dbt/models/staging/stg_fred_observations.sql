{{ config(materialized=cl_materialized('view')) }}

{#- Current version per (series_id, observation_date); FRED `.` placeholders (null values) dropped. -#}
with versions as (

    select
        *,
        row_number() over (partition by series_id, observation_date order by _loaded_at desc) as rn
    from {{ source('conductlens_raw', 'fred_observations') }}

)

select
    series_id,
    observation_date,
    value,
    _source,
    _source_as_of,
    _loaded_at
from versions
where rn = 1
  and not coalesce(_is_tombstone, false)
  and value is not null
