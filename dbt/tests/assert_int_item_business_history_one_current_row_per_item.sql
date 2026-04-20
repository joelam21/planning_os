{{ config(tags=['critical']) }}

-- Every item_number must have exactly one current row (is_current = true).
-- Zero current rows means the most recent version was never flagged.
-- Multiple current rows means the lead() window logic produced duplicate open-ended rows.

select
    item_number,
    count(*) as current_row_count
from {{ ref('int_item_business_history') }}
where is_current = true
group by item_number
having count(*) != 1
