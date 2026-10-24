{{ config(materialized=cl_materialized('table')) }}

{#-
  Every distinct (product, sub_product) pair observed in the complaints, mapped to a product_group via the
  product_groups seed. A seed row matches when `product` is equal and `sub_product_pattern` is empty (any
  sub-product) or is a lowercase substring of lower(sub_product). The most specific (longest) pattern wins.
  Unmatched pairs fall back to 'other' with is_matched = false (a warn test watches their volume).
-#}
with pairs as (

    -- read the per-target staging directly: int_complaints_daily depends on this model
    select
        product,
        sub_product,
{% if cl_cfpb_grain() == 'daily_cells' %}
        sum(n_complaints) as n_complaints
    from {{ ref('stg_cfpb_complaints_daily') }}
{% else %}
        count(*) as n_complaints
    from {{ ref('stg_cfpb_complaints') }}
{% endif %}
    group by 1, 2

),

seed as (

    select
        product,
        coalesce(sub_product_pattern, '') as sub_product_pattern,
        product_group
    from {{ ref('product_groups') }}

),

candidates as (

    select
        p.product,
        p.sub_product,
        p.n_complaints,
        s.product_group,
        s.sub_product_pattern,
        row_number() over (
            partition by p.product, p.sub_product
            order by length(s.sub_product_pattern) desc, s.product_group
        ) as rn
    from pairs as p
    left join seed as s
      on s.product = p.product
     and (
            s.sub_product_pattern = ''
         or lower(coalesce(p.sub_product, '')) like concat('%', s.sub_product_pattern, '%')
     )

)

select
    product,
    sub_product,
    coalesce(product_group, 'other') as product_group,
    product_group is not null as is_matched,
    nullif(sub_product_pattern, '') as matched_pattern,
    n_complaints
from candidates
where rn = 1
