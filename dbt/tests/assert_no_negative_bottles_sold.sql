-- bottles_sold must be non-negative
-- Zero is permitted (possible reporting artifact); negative is not
select *
from {{ ref('fct_liquor_sales') }}
where bottles_sold < 0