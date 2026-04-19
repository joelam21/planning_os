with max_date as (
    select date_trunc('week', max(order_date)) as current_week_start
    from {{ ref('int_iowa_liquor_sales_deduped') }}
),

week_spine as (
    select
        dateadd(week, -seq4() - 1, max_date.current_week_start) as week_start
    from table(generator(rowcount => 8))
    cross join max_date
),

weekly_depletions as (
    select
        date_trunc('week', order_date) as week_start,
        item_number,
        sum(bottles_sold) as weekly_units_sold
    from {{ ref('int_iowa_liquor_sales_deduped') }}
    where upper(invoice_item_number) not like 'RINV-%'
    group by 1, 2
),

weekly_scoped as (
    select
        weekly_depletions.week_start,
        weekly_depletions.item_number,
        weekly_depletions.weekly_units_sold
    from weekly_depletions
    cross join max_date
    where weekly_depletions.week_start >= dateadd(week, -8, max_date.current_week_start)
    and weekly_depletions.week_start < max_date.current_week_start
),

active_items as (
    select distinct item_number
    from weekly_scoped
),

item_week_grid as (
    select
        active_items.item_number,
        week_spine.week_start
    from active_items
    cross join week_spine
),

item_week_filled as (
    select
        item_week_grid.item_number,
        item_week_grid.week_start,
        coalesce(weekly_scoped.weekly_units_sold, 0) as weekly_units_sold
    from item_week_grid
    left join weekly_scoped
        on item_week_grid.item_number = weekly_scoped.item_number
        and item_week_grid.week_start = weekly_scoped.week_start
)

select *
from item_week_filled