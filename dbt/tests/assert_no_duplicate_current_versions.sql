{#-
  Staging must expose exactly one current version per business key on every source.
  The CFPB part is target-specific: complaint grain on Databricks, daily-cell grain on BigQuery.
-#}
{% if cl_cfpb_grain() == 'daily_cells' %}
select
    'stg_cfpb_complaints_daily' as model,
    concat(
        cast(received_date as {{ dbt.type_string() }}), '|', coalesce(company, ''), '|', coalesce(product, ''), '|',
        coalesce(sub_product, ''), '|', coalesce(issue, ''), '|', coalesce(state, ''), '|',
        coalesce(company_response, ''), '|', coalesce(timely_response, ''), '|', coalesce(consumer_disputed, '')
    ) as business_key,
    count(*) as n
from {{ ref('stg_cfpb_complaints_daily') }}
group by 1, 2
having count(*) > 1
{% else %}
select 'stg_cfpb_complaints' as model, cast(complaint_id as {{ dbt.type_string() }}) as business_key, count(*) as n
from {{ ref('stg_cfpb_complaints') }}
group by 1, 2
having count(*) > 1
{% endif %}

union all

select 'stg_fdic_institutions' as model, cast(cert as {{ dbt.type_string() }}) as business_key, count(*) as n
from {{ ref('stg_fdic_institutions') }}
group by 1, 2
having count(*) > 1

union all

select 'stg_fred_observations' as model, concat(series_id, ':', cast(observation_date as {{ dbt.type_string() }})) as business_key, count(*) as n
from {{ ref('stg_fred_observations') }}
group by 1, 2
having count(*) > 1
