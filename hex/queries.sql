-- ConductLens Hex dashboard queries.
-- One block per business question (Q1-Q10). All SQL is portable: it runs unchanged on Databricks (the
-- connection Hex is bound to) and on BigQuery, because mart names and columns are identical on both.
-- Tables are addressed as conductlens_marts.<mart>; set the Hex connection's default catalog/project
-- (Databricks: catalog `workspace`; BigQuery: project CL_BQ_PROJECT).
-- Hex input parameters are written as {{ param }} (Hex Jinja). Defaults are noted per block.
-- Dates are CFPB US-Eastern calendar dates; "today" in the marts is US/Eastern (see hex/README.md).

-- =====================================================================================================
-- Q1. How many complaints this week / month / year-to-date vs the same period last year?
-- Chart: 3 KPI tiles (complaints_current, with pct_change as the delta) + a small bar pair per period.
-- Inputs: none.
-- =====================================================================================================
select
    period_type,
    period_start,
    period_end,
    days_in_period,
    complaints_current,
    ly_period_start,
    ly_period_end,
    complaints_last_year,
    pct_change
from conductlens_marts.mart_volume_periods
order by case period_type when 'week' then 1 when 'month' then 2 else 3 end;

-- =====================================================================================================
-- Q2a. Which products are growing? Monthly volume and share by product group.
-- Chart: stacked area (received_month x complaints, colour = product_group) + line of yoy_growth.
-- Inputs: {{ start_month }} (date input, default = first of the month 36 months ago), {{ product_groups }}
--         (multi-select, default all). Date arithmetic is done in the Hex input, not in SQL, to keep the
--         text portable across Databricks and BigQuery.
-- Caption to show under the chart: "BNPL: no CFPB sub-product carries 'buy now, pay later' as of
--         2026-10, so the bnpl product group is empty; BNPL lenders are tracked in Q7 through the
--         digital-first segment (Affirm, Klarna, Sezzle, Zip, Afterpay/Block). Crypto = sub-product
--         'Virtual currency'; money transfer = domestic/international transfers and digital wallets."
-- =====================================================================================================
select
    received_month,
    product_group,
    complaints,
    industry_complaints,
    share_of_month,
    complaints_prior_year,
    yoy_growth,
    is_complete_month
from conductlens_marts.mart_product_trend_monthly
where received_month >= {{ start_month }}
  and product_group in ({{ product_groups }})
order by received_month, product_group;

-- =====================================================================================================
-- Q2b. Fastest-growing issues: trailing twelve months vs the prior twelve months.
-- Chart: horizontal bar of ttm_growth (top N by ttm_abs_change), table with ttm / prior_ttm.
-- Inputs: {{ product_group }} (single-select, default 'credit_card'), {{ min_ttm_complaints }} (int, default 500).
-- =====================================================================================================
select
    product_group,
    issue,
    ttm_complaints,
    prior_ttm_complaints,
    ttm_abs_change,
    ttm_growth
from conductlens_marts.mart_issue_trend_monthly
where is_latest_complete_month
  and product_group = {{ product_group }}
  and ttm_complaints >= {{ min_ttm_complaints }}
order by ttm_growth desc
limit 25;

-- =====================================================================================================
-- Q3. Is a company answering on time? Monthly and trailing-12-month timely share (CFPB 15-day flag).
-- Chart: line of timely_share and ttm_timely_share per month; bar of complaints on a secondary axis.
-- Inputs: {{ company }} (single-select from dim_company where complaints_ttm >= 100).
-- =====================================================================================================
select
    received_month,
    company,
    complaints,
    timely_known,
    timely_share,
    ttm_complaints,
    ttm_timely_share,
    timely_window_days,
    is_complete_month
from conductlens_marts.mart_company_timeliness_monthly
where company = {{ company }}
order by received_month;

-- =====================================================================================================
-- Q4. How does a company resolve complaints compared with the industry (trailing twelve months)?
-- Chart: paired bars per response_category (share_of_company vs industry_share).
-- Inputs: {{ company }}.
-- =====================================================================================================
select
    company,
    response_category,
    complaints,
    company_complaints_ttm,
    share_of_company,
    industry_complaints,
    industry_share,
    share_of_company - industry_share as share_vs_industry
from conductlens_marts.mart_company_resolution
where company = {{ company }}
order by share_of_company desc;

-- =====================================================================================================
-- Q5. How often did consumers dispute the company's answer, and how much of the data carries the flag?
-- Chart: line of dispute_rate by year for the company vs the industry ('ALL'), bar of coverage_share.
-- Inputs: {{ company }}. Note: the CFPB stopped collecting the flag on 2017-04-24; coverage_share
--         shows how much of each year is measurable.
-- =====================================================================================================
select
    received_year,
    company,
    is_industry,
    complaints,
    disputed_known,
    disputed_yes,
    dispute_rate,
    coverage_share
from conductlens_marts.mart_dispute_rate
where company in ({{ company }}, 'ALL')
  and disputed_known > 0
order by received_year, is_industry;

-- =====================================================================================================
-- Q6. Benchmark: complaints per $1bn of deposits within the company's size bucket.
-- Chart: ranked bar of complaints_per_bn_deposits for the bucket, our company highlighted
--        (is_our_company); a second table lists the fintech_unnormalised peer group by raw volume.
-- Inputs: {{ size_bucket }} (single-select, default = bucket of the is_our_company row),
--         {{ peer_group }} (default 'chartered').
-- =====================================================================================================
select
    company,
    holding_name,
    match_method,
    peer_group,
    size_bucket,
    complaints_ttm,
    bank_product_complaints_ttm,
    deposits_bn,
    complaints_per_bn_deposits,
    rank_in_bucket,
    n_in_bucket,
    is_our_company,
    is_digital_first,
    digital_segment
from conductlens_marts.mart_company_benchmark
where peer_group = {{ peer_group }}
  and size_bucket = {{ size_bucket }}
order by rank_in_bucket
limit 50;

-- =====================================================================================================
-- Q7. Digital-first fintechs vs chartered banks: service quality over time.
-- Chart: lines of timely_share by segment; stacked 100 % bar of the response-mix shares per segment
--        for the latest complete month.
-- Inputs: {{ start_month }} (date input, default = first of the month 36 months ago).
-- =====================================================================================================
select
    received_month,
    segment,
    complaints,
    timely_share,
    dispute_rate,
    dispute_coverage_share,
    share_explanation,
    share_monetary_relief,
    share_non_monetary_relief,
    share_closed_other,
    share_in_progress,
    share_untimely,
    is_complete_month
from conductlens_marts.mart_digital_vs_chartered
where received_month >= {{ start_month }}
order by received_month, segment;

-- =====================================================================================================
-- Q8. Early warning: which company x issue pairs spiked in the latest complete week?
-- Chart: table of spikes sorted by z_score; sparkline of this_week over the trailing 60 weeks for the
--        selected pair.
-- Inputs: {{ only_spikes }} (bool, default true), {{ company }} (optional filter), {{ issue }} (optional).
-- =====================================================================================================
select
    received_week,
    company,
    issue,
    this_week,
    trailing_8w_avg,
    trailing_8w_stddev,
    z_score,
    is_spike
from conductlens_marts.mart_weekly_spikes
where is_latest_week
  and ({{ only_spikes }} = false or is_spike)
order by z_score desc
limit 100;

-- Q8 detail: history for one pair (sparkline).
select
    received_week,
    this_week,
    trailing_8w_avg,
    z_score,
    is_spike,
    is_current_week
from conductlens_marts.mart_weekly_spikes
where company = {{ company }}
  and issue = {{ issue }}
order by received_week;

-- =====================================================================================================
-- Q9a. Do rates lead complaints? Weekly rate series next to weekly mortgage / credit-card complaints.
-- Chart: dual-axis line (avg_rate vs mortgage_complaints or credit_card_complaints) per series.
-- Inputs: {{ series_id }} (single-select, default 'MORTGAGE30US'), {{ start_week }} (date input, default = 5 years ago).
-- =====================================================================================================
select
    received_week,
    series_id,
    avg_rate,
    mortgage_complaints,
    credit_card_complaints,
    total_complaints,
    is_complete_week
from conductlens_marts.mart_rates_weekly
where series_id = {{ series_id }}
  and received_week >= {{ start_week }}
  and is_complete_week
order by received_week;

-- =====================================================================================================
-- Q9b. Lag correlation: correlation of 4-week complaint changes with rate changes lagged 0-26 weeks.
-- Chart: line of corr_changes by lag_weeks, one line per series_id, best lag marked (is_best_lag).
-- Inputs: {{ product_group }} ('mortgage' | 'credit_card').
-- =====================================================================================================
select
    product_group,
    series_id,
    lag_weeks,
    n_weeks,
    corr_levels,
    corr_changes,
    is_best_lag
from conductlens_marts.mart_rates_lag_correlation
where product_group = {{ product_group }}
order by series_id, lag_weeks;

-- =====================================================================================================
-- Q10. Where do complaints come from? Trailing-twelve-month complaints per 100k residents by state.
-- Chart: choropleth of state_per_100k; stacked bar of share_of_state by product_group per region.
-- Inputs: {{ product_group }} (optional; use 'all' to show state totals).
-- =====================================================================================================
select
    state,
    region,
    division,
    product_group,
    complaints,
    state_complaints,
    share_of_state,
    population,
    per_100k,
    state_per_100k
from conductlens_marts.mart_state_complaints
where ({{ product_group }} = 'all' or product_group = {{ product_group }})
order by state_per_100k desc, state, product_group;

-- =====================================================================================================
-- Honesty panel: how much complaint volume is behind each company-matching method.
-- Chart: table + 100 % bar of share_bank_product_ttm.
-- =====================================================================================================
select
    match_method,
    is_matched,
    n_companies,
    complaints_all_time,
    share_all_time,
    complaints_ttm,
    share_ttm,
    bank_product_complaints_ttm,
    share_bank_product_ttm
from conductlens_marts.mart_company_match_coverage
order by complaints_ttm desc;
