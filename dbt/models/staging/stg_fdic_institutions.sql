{{ config(materialized=cl_materialized('view')) }}

{#- Current version per FDIC charter; money in USD; holding_key = RSSDHCR or 'CERT-<cert>' for standalone charters. -#}
with versions as (

    select
        *,
        row_number() over (partition by cert order by _loaded_at desc) as rn
    from {{ source('conductlens_raw', 'fdic_institutions') }}

),

current_rows as (

    select * from versions
    where rn = 1
      and not coalesce(_is_tombstone, false)

),

cleaned as (

    select
        cert,
        name,
        city,
        stalp,
        bkclass,
        active,
        fed_rssd,
        nullif(nullif(trim(rssdhcr), ''), '0') as rssdhcr,
        nullif(trim(namehcr), '') as namehcr,
        asset_thousands,
        dep_thousands,
        offices,
        repdte,
        dateupdt,
        estymd,
        endefymd,
        webaddr,
        _source,
        _source_as_of,
        _loaded_at
    from current_rows

)

select
    cert,
    name,
    {{ normalize_company_name('name') }} as name_norm,
    city,
    stalp,
    bkclass,
    active,
    fed_rssd,
    rssdhcr,
    namehcr,
    {{ normalize_company_name('namehcr') }} as namehcr_norm,
    coalesce(rssdhcr, concat('CERT-', cast(cert as {{ dbt.type_string() }}))) as holding_key,
    rssdhcr is not null as has_holding_company,
    cast(asset_thousands as {{ dbt.type_bigint() }}) * 1000 as asset_usd,
    cast(dep_thousands as {{ dbt.type_bigint() }}) * 1000 as dep_usd,
    offices,
    repdte,
    dateupdt,
    estymd,
    endefymd,
    webaddr,
    _source,
    _source_as_of,
    _loaded_at
from cleaned
