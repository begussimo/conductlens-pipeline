{#- Spikes must satisfy their own definition, and a finite z must exist wherever 8 prior weeks exist. -#}
select
    received_week,
    company,
    issue,
    this_week,
    trailing_8w_avg,
    trailing_8w_stddev,
    z_score,
    is_spike
from {{ ref('mart_weekly_spikes') }}
where (trailing_8w_avg is not null and z_score is null)
   or (is_spike and not (z_score > 2 and trailing_8w_avg >= 5))
   or (not is_spike and coalesce(z_score > 2 and trailing_8w_avg >= 5, false))
