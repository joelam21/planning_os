{{ config(tags=['critical']) }}

-- For each item, no two validity windows should overlap.
-- An overlap means a given order_date could join to more than one historical version,
-- corrupting any point-in-time price or attribute lookup.
--
-- Given the lead()-based construction, overlaps can only occur if the same
-- business_valid_from appears twice for the same item. This test catches that
-- directly by detecting cases where one window contains another window's start date.

select
    a.item_number,
    a.business_valid_from as window_a_from,
    a.business_valid_to   as window_a_to,
    b.business_valid_from as window_b_from
from {{ ref('int_item_business_history') }} a
join {{ ref('int_item_business_history') }} b
    on  a.item_number = b.item_number
    and a.business_valid_from < b.business_valid_from
    and (
        a.business_valid_to is null
        or a.business_valid_to >= b.business_valid_from
    )
