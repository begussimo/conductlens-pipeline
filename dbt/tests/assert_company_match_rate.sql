{{ config(severity='warn') }}

{#-
  Honesty check: at least 85 % of trailing-twelve-month complaints about bank products (mortgage,
  credit card, checking/savings, auto, personal and student loans, prepaid) should come from companies
  mapped to an FDIC holding. Returns one row (fails -> warn) when the matched share is below 0.85.
-#}
with coverage as (

    select
        sum(case when is_matched then bank_product_complaints_ttm else 0 end) as matched,
        sum(bank_product_complaints_ttm) as total
    from {{ ref('mart_company_match_coverage') }}

)

select
    matched,
    total,
    {{ safe_divide('matched', 'total') }} as matched_share
from coverage
where {{ safe_divide('matched', 'total') }} < 0.85
