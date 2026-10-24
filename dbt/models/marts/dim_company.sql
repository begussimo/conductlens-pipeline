{{ config(materialized=cl_materialized('table')) }}

{#- One row per CFPB company string: mapping, flags, FDIC deposits and size bucket, complaint volumes (from int_complaints_daily). -#}
with mapping as (

    select * from {{ ref('int_company_mapping') }}

),

deposits as (

    select * from {{ ref('int_fdic_deposits_by_holding') }}

),

volumes as (

    select
        company,
        sum(n_complaints) as complaints_all_time,
        sum(case when received_date >= {{ cl_ttm_start() }} then n_complaints else 0 end) as complaints_ttm,
        sum(case
                when received_date >= {{ cl_ttm_start() }}
                 and product_group in {{ cl_bank_product_groups() }}
                then n_complaints else 0
            end) as bank_product_complaints_ttm,
        min(received_date) as first_received_date,
        max(received_date) as last_received_date
    from {{ ref('int_complaints_daily') }}
    group by company

)

select
    m.company,
    m.company_name_norm,
    m.holding_key,
    d.holding_name,
    m.match_method,
    m.is_matched,
    m.is_digital_first,
    m.digital_segment,
    m.seed_note,
    d.dep_usd,
    d.asset_usd,
    d.deposits_bn,
    d.n_charters,
    d.size_bucket,
    case
        when m.holding_key is not null and coalesce(d.dep_usd, 0) > 0 then 'chartered'
        else 'fintech_unnormalised'
    end as peer_group,
    coalesce(v.complaints_all_time, 0) as complaints_all_time,
    coalesce(v.complaints_ttm, 0) as complaints_ttm,
    coalesce(v.bank_product_complaints_ttm, 0) as bank_product_complaints_ttm,
    v.first_received_date,
    v.last_received_date,
    m.company = '{{ var("our_company") }}' as is_our_company
from mapping as m
left join deposits as d
  on d.holding_key = m.holding_key
left join volumes as v
  on v.company = m.company
