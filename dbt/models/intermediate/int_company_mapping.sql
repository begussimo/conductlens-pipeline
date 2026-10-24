{{ config(materialized=cl_materialized('table')) }}

{#-
  CFPB `company` text -> FDIC holding_key. Match order (first hit wins):
    1. seed      : company_fdic_map.csv with a non-empty holding_key (CERT-<cert> keys are resolved
                   to the charter's current holding_key)
    2. norm_name_holding : normalised company name = normalised FDIC holding-company name (namehcr)
    3. norm_name_charter : normalised company name = normalised FDIC charter name (name)
    4. unmatched
  Seed rows with an empty holding_key deliberately fall through to the name joins ("needs manual match").
  Digital-first flag/segment come from digital_first_companies.csv. Credit bureaus, debt collectors and
  servicers are not banks and are expected to stay unmatched.
-#}
with companies as (

    -- distinct company strings from the per-target staging (int_complaints_daily depends on this model;
    -- complaint volumes per company live in dim_company, computed from int_complaints_daily)
    select
        company,
        max(company_name_norm) as company_name_norm
{% if cl_cfpb_grain() == 'daily_cells' %}
    from {{ ref('stg_cfpb_complaints_daily') }}
{% else %}
    from {{ ref('stg_cfpb_complaints') }}
{% endif %}
    group by company

),

seed_raw as (

    select
        upper(trim(cfpb_company)) as cfpb_company_key,
        nullif(trim(holding_key), '') as holding_key,
        note
    from {{ ref('company_fdic_map') }}

),

-- a seed key of the form CERT-<cert> names a lead charter; resolve it to that charter's current
-- holding_key (its RSSDHCR when it has a holding company) so deposits roll up correctly
seed_map as (

    select
        s.cfpb_company_key,
        coalesce(f.holding_key, s.holding_key) as holding_key,
        s.note
    from seed_raw as s
    left join {{ ref('stg_fdic_institutions') }} as f
      on s.holding_key = concat('CERT-', cast(f.cert as {{ dbt.type_string() }}))

),

digital as (

    select
        upper(trim(cfpb_company)) as cfpb_company_key,
        min(digital_segment) as digital_segment
    from {{ ref('digital_first_companies') }}
    group by 1

),

fdic as (

    select * from {{ ref('stg_fdic_institutions') }}

),

-- one holding_key per normalised holding-company name (largest deposits wins on collisions)
holdings_by_name as (

    select
        namehcr_norm,
        holding_key
    from (
        select
            namehcr_norm,
            holding_key,
            row_number() over (
                partition by namehcr_norm
                order by sum(case when active then coalesce(dep_usd, 0) else 0 end) desc, holding_key
            ) as rn
        from fdic
        where has_holding_company
          and namehcr_norm is not null
        group by namehcr_norm, holding_key
    ) as ranked
    where rn = 1

),

-- one holding_key per normalised charter name (active and largest deposits wins)
charters_by_name as (

    select
        name_norm,
        holding_key
    from (
        select
            name_norm,
            holding_key,
            row_number() over (
                partition by name_norm
                order by case when active then 0 else 1 end, coalesce(dep_usd, 0) desc, cert
            ) as rn
        from fdic
        where name_norm is not null
    ) as ranked
    where rn = 1

),

joined as (

    select
        c.company,
        c.company_name_norm,
        s.holding_key as seed_holding_key,
        s.note as seed_note,
        h.holding_key as holding_name_key,
        ch.holding_key as charter_name_key,
        d.digital_segment
    from companies as c
    left join seed_map as s
      on s.cfpb_company_key = upper(trim(c.company))
    left join holdings_by_name as h
      on h.namehcr_norm = c.company_name_norm
    left join charters_by_name as ch
      on ch.name_norm = c.company_name_norm
    left join digital as d
      on d.cfpb_company_key = upper(trim(c.company))

)

select
    company,
    company_name_norm,
    coalesce(seed_holding_key, holding_name_key, charter_name_key) as holding_key,
    case
        when seed_holding_key is not null then 'seed'
        when holding_name_key is not null then 'norm_name_holding'
        when charter_name_key is not null then 'norm_name_charter'
        else 'unmatched'
    end as match_method,
    coalesce(seed_holding_key, holding_name_key, charter_name_key) is not null as is_matched,
    digital_segment is not null as is_digital_first,
    digital_segment,
    seed_note
from joined
