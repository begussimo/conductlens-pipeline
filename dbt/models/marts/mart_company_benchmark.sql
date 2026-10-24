{{ config(materialized=cl_materialized('table')) }}

{#-
  q6: TTM complaints per company, normalised by FDIC deposits where a chartered holding is matched.
    peer_group = 'chartered'            : holding_key matched and deposits > 0; rank within size_bucket by
                                          complaints_per_bn_deposits (dense_rank desc)
    peer_group = 'fintech_unnormalised' : no deposits to normalise by (fintechs, bureaus, collectors);
                                          ranked by raw complaints_ttm within bucket 'unnormalised'
  is_our_company flags var('our_company').
-#}
with companies as (

    select * from {{ ref('dim_company') }}
    where complaints_ttm > 0

),

scored as (

    select
        company,
        holding_key,
        holding_name,
        match_method,
        is_digital_first,
        digital_segment,
        peer_group,
        complaints_ttm,
        bank_product_complaints_ttm,
        case when peer_group = 'chartered' then deposits_bn end as deposits_bn,
        case when peer_group = 'chartered' then {{ safe_divide('complaints_ttm', 'deposits_bn') }} end as complaints_per_bn_deposits,
        case when peer_group = 'chartered' then size_bucket else 'unnormalised' end as size_bucket,
        is_our_company
    from companies

)

select
    company,
    holding_key,
    holding_name,
    match_method,
    is_digital_first,
    digital_segment,
    peer_group,
    complaints_ttm,
    bank_product_complaints_ttm,
    deposits_bn,
    complaints_per_bn_deposits,
    size_bucket,
    case
        when peer_group = 'chartered' then
            dense_rank() over (
                partition by peer_group, size_bucket
                order by complaints_per_bn_deposits desc
            )
        else
            dense_rank() over (
                partition by peer_group, size_bucket
                order by complaints_ttm desc
            )
    end as rank_in_bucket,
    count(*) over (partition by peer_group, size_bucket) as n_in_bucket,
    is_our_company
from scored
