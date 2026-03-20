-- sale_dollars must be non-negative
-- A negative value indicates a data quality issue upstream
select *
from {{ ref('fct_liquor_sales') }}
where sale_dollars < 0