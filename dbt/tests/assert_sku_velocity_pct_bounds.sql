select *
from {{ ref('fct_sku_velocity') }}
where
    cumulative_pct < 0 or cumulative_pct > 1
    or unit_share_pct < 0 or unit_share_pct > 1
    or revenue_share_pct < 0 or revenue_share_pct > 1