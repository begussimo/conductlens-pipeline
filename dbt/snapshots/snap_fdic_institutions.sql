{% snapshot snap_fdic_institutions %}

{{
    config(
        enabled=(target.type == 'databricks'),
        unique_key='cert',
        strategy='check',
        check_cols=['name', 'namehcr', 'rssdhcr', 'active', 'dep_thousands', 'asset_thousands'],
        invalidate_hard_deletes=true
    )
}}

{#-
  Slowly-changing history of FDIC charters (names, holding company, status, deposits, assets).
  Databricks only: snapshots need MERGE, which the BigQuery sandbox does not allow.
  target schema comes from `snapshots: +schema: snapshots` -> <prefix>_snapshots.
-#}
select
    cert,
    name,
    city,
    stalp,
    bkclass,
    active,
    fed_rssd,
    rssdhcr,
    namehcr,
    cast(asset_usd / 1000 as {{ dbt.type_bigint() }}) as asset_thousands,
    cast(dep_usd / 1000 as {{ dbt.type_bigint() }}) as dep_thousands,
    offices,
    repdte,
    dateupdt,
    estymd,
    endefymd,
    webaddr,
    _loaded_at
from {{ ref('stg_fdic_institutions') }}

{% endsnapshot %}
