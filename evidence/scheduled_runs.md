# Scheduled runs (generated 2026-10-24 19:48 UTC)

## bigquery

| started (UTC) | run_id | source | mode | status | window | read | written | seconds | error |
|---|---|---|---|---|---|---:|---:|---:|---|
| 2026-10-24 19:30 | `scheduled__2026-10-23T13:00:00+00:00` | fdic | full | success | full | 27835 | 0 | 5 |  |
| 2026-10-24 19:30 | `scheduled__2026-10-23T13:00:00+00:00` | cfpb | incremental | success | 2026-08-25..2026-10-23 | 150568 | 0 | 37 |  |
| 2026-10-24 19:30 | `scheduled__2026-10-23T13:00:00+00:00` | fred | incremental | success | 2026-07-26..2026-10-24 | 230 | 0 | 6 |  |
| 2026-10-24 19:14 | `bq-rebuild-cells` | cfpb | rehydrate | success | 2011-12-01..2026-10-24 | 5163903 | 5163903 | 611 |  |
| 2026-10-24 19:09 | `bq-initial-load-2` | fred | incremental | success | 2011-12-01..2026-10-24 | 14164 | 14164 | 9 |  |
| 2026-10-24 19:09 | `bq-initial-load-2` | fdic | full | success | full | 27835 | 27835 | 7 |  |
| 2026-10-24 19:07 | `bq-initial-load` | fred | incremental | failed | full | 0 | 0 | 2 | 403 request failed: the user does not have 'bigquery.readses |
| 2026-10-24 19:06 | `bq-initial-load` | fdic | full | failed | full | 27835 | 0 | 2 | 403 request failed: the user does not have 'bigquery.readses |
| 2026-10-24 18:55 | `bq-initial-load` | cfpb | rehydrate | success | 2011-12-01..2026-10-24 | 5163903 | 5163903 | 678 |  |

### parity checks

| checked (UTC) | table | metric | bigquery | databricks | rel diff | status |
|---|---|---|---:|---:|---:|---|
| 2026-10-24 19:48 | mart_company_timeliness_monthly | sum_timely_share | 16219.2 | 16219.2 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | count | 521 | 521 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | sum_dispute_rate | 0 | 0 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_benchmark | count | 4151 | 4151 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_issue_trend_monthly | count | 6245 | 6245 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_resolution | count | 1081 | 1081 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_resolution | sum_share_of_company | 474 | 474 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_dispute_rate | sum_coverage_share | 0 | 0 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_timeliness_monthly | sum_ttm_timely_share | 16279.1 | 16279.1 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | sum_share_monetary_relief | 39.5327 | 39.5327 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | sum_timely_share | 513.385 | 513.385 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_rates_lag_correlation | sum_lag_weeks | 3510 | 3510 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_match_coverage | sum_share_bank_product_ttm | 1 | 1 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_benchmark | sum_rank_in_bucket | 1.61847e+06 | 1.61847e+06 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_product_trend_monthly | sum_yoy_growth | 1488.11 | 1488.11 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_product_trend_monthly | count | 1900 | 1900 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_timeliness_monthly | sum_timely_window_days | 255195 | 255195 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_rates_lag_correlation | count | 270 | 270 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_issue_trend_monthly | sum_ttm_growth | 2325.23 | 2325.23 | 0.0000% | ok |
| 2026-10-24 19:48 | dim_company | count | 8189 | 8189 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_benchmark | sum_complaints_per_bn_deposits | 2262.65 | 2262.65 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | sum_dispute_coverage_share | 0 | 0 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_match_coverage | sum_share_all_time | 1 | 1 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | sum_share_closed_other | 12.9955 | 12.9955 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_dispute_rate | sum_received_year | 8.94538e+07 | 8.94538e+07 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | sum_share_in_progress | 2.27991 | 2.27991 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | sum_share_non_monetary_relief | 74.9036 | 74.9036 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_dispute_rate | sum_dispute_rate | 0 | 0 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_rates_lag_correlation | sum_corr_levels | 54.9485 | 54.9485 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_resolution | sum_industry_share | 354.427 | 354.427 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_dispute_rate | count | 44282 | 44282 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_rates_lag_correlation | sum_corr_changes | -1.50028 | -1.50028 | 0.0000% | ok |
| 2026-10-24 19:48 | dim_company | sum_deposits_bn | 17588.4 | 17588.4 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | sum_share_explanation | 390.607 | 390.607 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_match_coverage | sum_share_ttm | 1 | 1 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | sum_share_untimely | 0.680828 | 0.680828 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_timeliness_monthly | count | 17013 | 17013 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_product_trend_monthly | sum_share_of_month | 179 | 179 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_match_coverage | count | 4 | 4 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_benchmark | sum_deposits_bn | 16260.7 | 16260.7 | 0.0000% | ok |

## databricks

| started (UTC) | run_id | source | mode | status | window | read | written | seconds | error |
|---|---|---|---|---|---|---:|---:|---:|---|
| 2026-10-24 19:31 | `scheduled__2026-10-23T13:00:00+00:00` | cfpb | incremental | success | 2026-08-24..2026-10-23 | 1322309 | 0 | 26 |  |
| 2026-10-24 19:30 | `scheduled__2026-10-23T13:00:00+00:00` | fdic | full | success | full | 27835 | 0 | 4 |  |
| 2026-10-24 19:30 | `scheduled__2026-10-23T13:00:00+00:00` | fred | incremental | success | 2026-07-26..2026-10-24 | 230 | 0 | 4 |  |
| 2026-10-24 19:01 | `dbx-initial-load-fred` | fred | incremental | success | 2011-12-01..2026-10-24 | 14164 | 14164 | 8 |  |
| 2026-10-24 19:01 | `dbx-initial-load` | fred | incremental | failed | 2011-12-01..2026-10-24 | 0 | 0 | 2 | Failed to parse string: '' as a scalar of type double |
| 2026-10-24 19:00 | `dbx-initial-load` | fdic | full | success | full | 27835 | 27835 | 6 |  |
| 2026-10-24 18:52 | `dbx-initial-load` | cfpb | incremental | success | 2011-12-01..2026-10-23 | 18653525 | 18653525 | 516 |  |

### parity checks

| checked (UTC) | table | metric | bigquery | databricks | rel diff | status |
|---|---|---|---:|---:|---:|---|
| 2026-10-24 19:48 | mart_company_benchmark | count | 4151 | 4151 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | sum_timely_share | 513.385 | 513.385 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_timeliness_monthly | sum_timely_share | 16219.2 | 16219.2 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_product_trend_monthly | count | 1900 | 1900 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_benchmark | sum_rank_in_bucket | 1.61847e+06 | 1.61847e+06 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_resolution | sum_industry_share | 354.427 | 354.427 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_match_coverage | sum_share_ttm | 1 | 1 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_timeliness_monthly | sum_timely_window_days | 255195 | 255195 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_match_coverage | sum_share_all_time | 1 | 1 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_match_coverage | sum_share_bank_product_ttm | 1 | 1 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_dispute_rate | count | 44282 | 44282 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_resolution | count | 1081 | 1081 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | sum_share_in_progress | 2.27991 | 2.27991 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_resolution | sum_share_of_company | 474 | 474 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_issue_trend_monthly | sum_ttm_growth | 2325.23 | 2325.23 | 0.0000% | ok |
| 2026-10-24 19:48 | dim_company | count | 8189 | 8189 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_match_coverage | count | 4 | 4 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_benchmark | sum_complaints_per_bn_deposits | 2262.65 | 2262.65 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_product_trend_monthly | sum_share_of_month | 179 | 179 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | sum_share_closed_other | 12.9955 | 12.9955 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_product_trend_monthly | sum_yoy_growth | 1488.11 | 1488.11 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | count | 521 | 521 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_dispute_rate | sum_dispute_rate | 0 | 0 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_timeliness_monthly | count | 17013 | 17013 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_dispute_rate | sum_coverage_share | 0 | 0 | 0.0000% | ok |
| 2026-10-24 19:48 | dim_company | sum_deposits_bn | 17588.4 | 17588.4 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | sum_share_non_monetary_relief | 74.9036 | 74.9036 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | sum_share_untimely | 0.680828 | 0.680828 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | sum_share_monetary_relief | 39.5327 | 39.5327 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_benchmark | sum_deposits_bn | 16260.7 | 16260.7 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_rates_lag_correlation | sum_corr_levels | 54.9485 | 54.9485 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | sum_dispute_rate | 0 | 0 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_issue_trend_monthly | count | 6245 | 6245 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | sum_dispute_coverage_share | 0 | 0 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_rates_lag_correlation | sum_corr_changes | -1.50028 | -1.50028 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_dispute_rate | sum_received_year | 8.94538e+07 | 8.94538e+07 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_company_timeliness_monthly | sum_ttm_timely_share | 16279.1 | 16279.1 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_digital_vs_chartered | sum_share_explanation | 390.607 | 390.607 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_rates_lag_correlation | count | 270 | 270 | 0.0000% | ok |
| 2026-10-24 19:48 | mart_rates_lag_correlation | sum_lag_weeks | 3510 | 3510 | 0.0000% | ok |
