{{ config(materialized=cl_materialized('table')) }}

{#-
  q7: month x segment service-quality comparison.
    digital_first : company in the digital_first_companies seed
    chartered     : company mapped to an FDIC holding_key (and not digital-first)
    other         : everything else (bureaus, collectors, servicers, unmatched)
-#}
with base as (

    select
        received_month,
        case
            when is_digital_first then 'digital_first'
            when holding_key is not null then 'chartered'
            else 'other'
        end as segment,
        is_timely,
        is_disputed,
        response_category,
        n_complaints
    from {{ ref('int_complaints_daily') }}

),

agg as (

    select
        received_month,
        segment,
        sum(n_complaints) as complaints,
        sum(case when is_timely then n_complaints else 0 end) as timely_complaints,
        sum(case when is_timely is not null then n_complaints else 0 end) as timely_known,
        sum(case when is_disputed then n_complaints else 0 end) as disputed_yes,
        sum(case when is_disputed is not null then n_complaints else 0 end) as disputed_known,
        sum(case when response_category = 'explanation' then n_complaints else 0 end) as n_explanation,
        sum(case when response_category = 'monetary_relief' then n_complaints else 0 end) as n_monetary_relief,
        sum(case when response_category = 'non_monetary_relief' then n_complaints else 0 end) as n_non_monetary_relief,
        sum(case when response_category = 'closed_other' then n_complaints else 0 end) as n_closed_other,
        sum(case when response_category = 'in_progress' then n_complaints else 0 end) as n_in_progress,
        sum(case when response_category = 'untimely' then n_complaints else 0 end) as n_untimely
    from base
    group by 1, 2

)

select
    received_month,
    segment,
    complaints,
    {{ safe_divide('timely_complaints', 'timely_known') }} as timely_share,
    {{ safe_divide('disputed_yes', 'disputed_known') }} as dispute_rate,
    {{ safe_divide('disputed_known', 'complaints') }} as dispute_coverage_share,
    {{ safe_divide('n_explanation', 'complaints') }} as share_explanation,
    {{ safe_divide('n_monetary_relief', 'complaints') }} as share_monetary_relief,
    {{ safe_divide('n_non_monetary_relief', 'complaints') }} as share_non_monetary_relief,
    {{ safe_divide('n_closed_other', 'complaints') }} as share_closed_other,
    {{ safe_divide('n_in_progress', 'complaints') }} as share_in_progress,
    {{ safe_divide('n_untimely', 'complaints') }} as share_untimely,
    received_month < {{ cl_month_start(cl_today()) }} as is_complete_month
from agg
