{{ config(materialized=cl_materialized('table')) }}

{#-
  q5: consumer dispute rate per company x year, plus an industry row (company = 'ALL').
  `consumer_disputed` only exists for API-sourced rows and the CFPB stopped collecting it on 2017-04-24,
  so `coverage_share` (rows with a non-null flag) is published next to the rate.
-#}
with base as (

    select
        received_year,
        company,
        consumer_disputed,
        n_complaints
    from {{ ref('int_complaints_daily') }}

),

per_company as (

    select
        received_year,
        company,
        sum(n_complaints) as complaints,
        sum(case when consumer_disputed in ('Yes', 'No') then n_complaints else 0 end) as disputed_known,
        sum(case when consumer_disputed = 'Yes' then n_complaints else 0 end) as disputed_yes
    from base
    group by 1, 2

),

industry as (

    select
        received_year,
        'ALL' as company,
        sum(n_complaints) as complaints,
        sum(case when consumer_disputed in ('Yes', 'No') then n_complaints else 0 end) as disputed_known,
        sum(case when consumer_disputed = 'Yes' then n_complaints else 0 end) as disputed_yes
    from base
    group by 1

),

unioned as (

    select *, false as is_industry from per_company
    union all
    select *, true as is_industry from industry

)

select
    received_year,
    company,
    is_industry,
    complaints,
    disputed_known,
    disputed_yes,
    {{ safe_divide('disputed_yes', 'disputed_known') }} as dispute_rate,
    {{ safe_divide('disputed_known', 'complaints') }} as coverage_share
from unioned
