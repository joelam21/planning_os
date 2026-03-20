-- fct_store_daily_sales grain is store_number × order_date
-- Any duplicate at this grain means the aggregation broke
select
    store_number,
    order_date,
    count(*) as row_count
from {{ ref('fct_store_daily_sales') }}
group by store_number, order_date
having count(*) > 1