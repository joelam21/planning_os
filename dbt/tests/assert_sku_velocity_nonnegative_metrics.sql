select *
from {{ ref('fct_sku_velocity') }}
where total_units_sold < 0 or total_revenue < 0