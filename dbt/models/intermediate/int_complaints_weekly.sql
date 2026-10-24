{{ config(materialized=cl_materialized('table')) }}

{#- Week (Monday) x company x product_group x issue counts; feeds spikes (q8) and rates (q9). -#}
select
    received_week,
    company,
    product_group,
    issue,
    sum(n_complaints) as n_complaints,
    sum(case when is_timely then n_complaints else 0 end) as n_timely,
    sum(case when is_timely is not null then n_complaints else 0 end) as n_timely_known
from {{ ref('int_complaints_daily') }}
group by 1, 2, 3, 4
