{{ config(severity='warn') }}

{#- Warn when (product, sub_product) pairs that fell back to `other` carry more than 1 % of all complaints. -#}
with totals as (

    select
        sum(case when not is_matched then n_complaints else 0 end) as unmatched,
        sum(n_complaints) as total
    from {{ ref('int_product_groups') }}

)

select unmatched, total, {{ safe_divide('unmatched', 'total') }} as unmatched_share
from totals
where {{ safe_divide('unmatched', 'total') }} > 0.01
