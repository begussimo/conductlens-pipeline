{{ config(materialized=cl_materialized('table')) }}

{#-
  Honesty table: how much of the complaint volume sits behind each company-matching method, overall,
  trailing twelve months, and for bank products only (the ones where FDIC normalisation is meaningful).
-#}
with base as (

    select
        match_method,
        company,
        received_date >= {{ cl_ttm_start() }} as is_ttm,
        product_group in {{ cl_bank_product_groups() }} as is_bank_product,
        n_complaints
    from {{ ref('int_complaints_daily') }}

),

agg as (

    select
        match_method,
        count(distinct company) as n_companies,
        sum(n_complaints) as complaints_all_time,
        sum(case when is_ttm then n_complaints else 0 end) as complaints_ttm,
        sum(case when is_ttm and is_bank_product then n_complaints else 0 end) as bank_product_complaints_ttm
    from base
    group by 1

)

select
    match_method,
    match_method <> 'unmatched' as is_matched,
    n_companies,
    complaints_all_time,
    {{ safe_divide('complaints_all_time', 'sum(complaints_all_time) over ()') }} as share_all_time,
    complaints_ttm,
    {{ safe_divide('complaints_ttm', 'sum(complaints_ttm) over ()') }} as share_ttm,
    bank_product_complaints_ttm,
    {{ safe_divide('bank_product_complaints_ttm', 'sum(bank_product_complaints_ttm) over ()') }} as share_bank_product_ttm
from agg
