{{ config(tags=['critical']) }}

-- (item_number, business_valid_from) is the grain of this model.
-- A duplicate at this grain means two change points were detected on the same date
-- for the same item, which would produce non-deterministic point-in-time joins.

select
    item_number,
    business_valid_from,
    count(*) as row_count
from {{ ref('int_item_business_history') }}
group by item_number, business_valid_from
having count(*) > 1
