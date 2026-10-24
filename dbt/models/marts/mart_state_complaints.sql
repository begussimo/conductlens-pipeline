{{ config(materialized=cl_materialized('table')) }}

{#-
  q10: TTM complaints per state x product_group with per-100k-population rates (seed population) and
  Census region/division. Restricted to the 50 states + DC + PR present in the state_regions seed; other
  CFPB `state` values (APO/FPO codes, territories, free text) are excluded.
-#}
with ttm as (

    select
        state,
        product_group,
        n_complaints
    from {{ ref('int_complaints_daily') }}
    where received_date >= {{ cl_ttm_start() }}
      and state is not null

),

by_state_product as (

    select
        state,
        product_group,
        sum(n_complaints) as complaints
    from ttm
    group by 1, 2

),

with_state_total as (

    select
        state,
        product_group,
        complaints,
        sum(complaints) over (partition by state) as state_complaints
    from by_state_product

),

population as (

    select state, population, source as population_source
    from {{ ref('state_population') }}

),

regions as (

    select state, region, division
    from {{ ref('state_regions') }}

)

select
    s.state,
    s.product_group,
    s.complaints,
    s.state_complaints,
    {{ safe_divide('s.complaints', 's.state_complaints') }} as share_of_state,
    p.population,
    {{ safe_divide('s.complaints * 100000.0', 'p.population') }} as per_100k,
    {{ safe_divide('s.state_complaints * 100000.0', 'p.population') }} as state_per_100k,
    r.region,
    r.division,
    p.population_source
from with_state_total as s
join regions as r
  on r.state = s.state
left join population as p
  on p.state = s.state
