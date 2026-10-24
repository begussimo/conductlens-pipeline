{{ config(
    enabled=(cl_cfpb_grain() == 'complaints'),
    materialized=cl_materialized('view')
) }}

{#-
  Databricks only (the BigQuery sandbox lands daily cells instead, see stg_cfpb_complaints_daily).
  Current version of every CFPB complaint: newest `_loaded_at` per complaint_id, tombstones removed.
  Timeliness: the CFPB's `Timely response?` flag records whether the company responded within the
  15-calendar-day regulatory window (var timely_window_days). We expose it as `is_timely` and do NOT
  recompute it, because the CFPB does not publish the company response date.
-#}
with versions as (

    select
        *,
        row_number() over (partition by complaint_id order by _loaded_at desc) as rn
    from {{ source('conductlens_raw', 'cfpb_complaints') }}

),

current_rows as (

    select * from versions
    where rn = 1
      and not coalesce(_is_tombstone, false)

)

select
    complaint_id,
    received_date,
    received_month_id,
    product,
    sub_product,
    issue,
    sub_issue,
    company,
    {{ normalize_company_name('company') }} as company_name_norm,
    company_public_response,
    state,
    zip_code,
    tags,
    submitted_via,
    date_sent_to_company,
    company_response,
    {{ cl_response_category('company_response') }} as response_category,
    timely_response,
    {{ cl_yes_no_bool('timely_response') }} as is_timely,
    consumer_disputed,
    {{ cl_yes_no_bool('consumer_disputed') }} as is_disputed,
    consumer_consent_provided,
    has_narrative,
    {{ dbt.datediff('received_date', 'date_sent_to_company', 'day') }} as days_to_forward,
    _source,
    _source_as_of,
    _loaded_at
from current_rows
