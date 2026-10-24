# ConductLens Hex app

The public dashboard is a Hex (Community plan) app that reads the `conductlens_marts` tables built by
the dbt project in `../dbt`. Every query it runs is in [`queries.sql`](queries.sql), one block per
business question, with the chart type and the Hex input parameters in the header comment.

## Connection

Bind the app to the **Databricks** connection (catalog `workspace`, schema `conductlens_marts`).
Databricks is the durable replica: the BigQuery sandbox expires its tables every 60 days and has a
lifetime write quota, so it is the brief's warehouse but not something a public app should depend on.
Mart names and columns are identical on both warehouses, so if the app is ever rebound to BigQuery no
query text changes.

In Hex: Settings → Data connections → Databricks → host (`DATABRICKS_HOST` without `https://`),
HTTP path of the SQL warehouse, personal access token. Set the default catalog to `workspace` so the
`conductlens_marts.<table>` names in the queries resolve.

## Layout

| Section | Questions | Cells |
|---|---|---|
| Volume & trend | Q1, Q2a, Q2b | three KPI tiles (WTD / MTD / YTD with Δ vs last year); stacked area by product group; "fastest-growing issues" bar |
| Service quality | Q3, Q4, Q5 | company selector (`company` input, options from `dim_company` with `complaints_ttm >= 100`); timely-share lines; response-mix paired bars; dispute-rate line with coverage bar |
| Benchmarking | Q6 | size-bucket selector defaulting to the `is_our_company` row's bucket; ranked bar of complaints per $1bn deposits with our company highlighted; secondary table for `fintech_unnormalised` |
| Risk & early warning | Q7, Q8 | digital-first vs chartered lines; spike table for the latest complete week (`is_latest_week`), click-through sparkline per company × issue |
| Geography | Q10 | choropleth of complaints per 100k; region/product-mix stacked bars |
| Rates | Q9a, Q9b | dual-axis rate vs complaints; lag-correlation lines with `is_best_lag` marker |
| Footer | honesty panel | `mart_company_match_coverage` table and the time-zone note below |

Input parameters used across cells: `company`, `product_group`, `product_groups`, `size_bucket`,
`peer_group`, `series_id`, `start_month`, `start_week`, `min_ttm_complaints`, `only_spikes`, `issue`.
Date windows are Hex date inputs (not SQL date arithmetic) so the same query text runs on both warehouses.

## Publishing publicly (Hex Community)

1. Build the app in a project; set every input cell's default so the app renders with no interaction.
2. Project settings → **Sharing** → "Anyone with the link can view" (Community plan: public link, no
   login). Choose "Publish" and "Publish app" for the current version.
3. Enable **scheduled runs** (daily, after the 13:00 UTC Airflow run; e.g. 14:30 UTC) so the published
   app is refreshed from cached results; viewers do not hit Databricks on each page load.
4. Turn off "Allow viewers to see the logic view" if only the app should be visible.
5. Paste the public URL into `dbt/models/exposures.yml` (`hex_conductlens_dashboard.url`).

## Time-zone note (display on the app)

> CFPB complaint dates are US-Eastern calendar dates with no time component. "This week", "this month"
> and "year to date" are computed against today's date in US/Eastern, so a refresh at 13:00 UTC and one
> at 23:30 UTC agree on what "today" is. Weeks start on Monday. The CFPB publishes a complaint only after
> it has been sent to the company, so the most recent few days are always incomplete. Timeliness is the
> CFPB's own 15-calendar-day flag; it is not recomputed because the company response date is not
> published.

## Captions the app must carry

- **Q2 (product mix):** the `bnpl` product group is empty because no CFPB sub-product currently
  contains "buy now, pay later"; BNPL companies are visible through the digital-first segment (Q7).
- **Q5 (dispute rate):** the CFPB stopped collecting "consumer disputed?" on 2017-04-24 and the bulk
  export no longer carries the column; `coverage_share` says how much of each year is measurable.
- **Q6 (benchmark):** the fintech table is unnormalised on purpose: fintechs have no FDIC deposits.
- **Time zone:** all dates are CFPB US-Eastern calendar dates; "today" is US/Eastern.
