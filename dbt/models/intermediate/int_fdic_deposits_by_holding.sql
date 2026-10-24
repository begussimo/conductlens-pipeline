{{ config(materialized=cl_materialized('table')) }}

{#- Active charters rolled up to holding_key; deposits/assets in USD; size bucket by total deposits. -#}
with active_charters as (

    select * from {{ ref('stg_fdic_institutions') }}
    where active

),

rolled as (

    select
        holding_key,
        max(case when has_holding_company then namehcr else name end) as holding_name,
        max(case when has_holding_company then 1 else 0 end) = 1 as is_holding_company,
        sum(coalesce(dep_usd, 0)) as dep_usd,
        sum(coalesce(asset_usd, 0)) as asset_usd,
        count(*) as n_charters,
        sum(coalesce(offices, 0)) as n_offices,
        max(repdte) as repdte
    from active_charters
    group by holding_key

)

select
    holding_key,
    holding_name,
    is_holding_company,
    dep_usd,
    asset_usd,
    dep_usd / 1000000000.0 as deposits_bn,
    n_charters,
    n_offices,
    repdte,
    {{ cl_size_bucket('dep_usd') }} as size_bucket
from rolled
