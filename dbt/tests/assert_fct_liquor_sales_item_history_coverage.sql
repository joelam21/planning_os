{{ config(tags=['critical']) }}

-- Every item_number in fct_liquor_sales must have at least one row in
-- int_item_business_history. This ensures no "orphan" items appear in the
-- fact table without any attribute history.
--
-- Date-specific coverage is not enforced here: int_item_business_history is a
-- full-rebuild model derived from the current ingestion window, so older
-- accumulated fact rows will predate the earliest history record. Point-in-time
-- join correctness is validated separately for the history model itself via the
-- no_overlapping_windows and one_current_row tests.

select distinct
    f.item_number
from {{ ref('fct_liquor_sales') }} f
where not exists (
    select 1
    from {{ ref('int_item_business_history') }} h
    where h.item_number = f.item_number
)
