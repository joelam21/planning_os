select *
from {{ ref('fct_sku_velocity') }}
where
    cumulative_volume_pct < 0 or cumulative_volume_pct > 1
    or cumulative_revenue_pct < 0 or cumulative_revenue_pct > 1