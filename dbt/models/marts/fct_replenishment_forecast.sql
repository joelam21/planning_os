with max_date as (
select date_trunc('week', max(order_date)) as current_week_start
from {{ ref('fct_liquor_sales') }}
),

week_spine as (
select
dateadd(week, -seq4() - 1, m.current_week_start) as week_start
from table(generator(rowcount => 8))
cross join max_date m
),

weekly_depletions as (
select
date_trunc('week', order_date) as week_start,
item_number,
sum(bottles_sold) as weekly_units_sold
from {{ ref('fct_liquor_sales') }}
where upper(invoice_item_number) not like 'RINV-%'
group by 1, 2
),

weekly_scoped as (
select
w.week_start,
w.item_number,
w.weekly_units_sold
from weekly_depletions w
cross join max_date m
where w.week_start >= dateadd(week, -8, m.current_week_start)
and w.week_start < m.current_week_start
),

active_items as (
select distinct item_number
from weekly_scoped
),

item_week_grid as (
select
i.item_number,
s.week_start
from active_items i
cross join week_spine s
),

item_week_filled as (
select
g.item_number,
g.week_start,
coalesce(ws.weekly_units_sold, 0) as weekly_units_sold
from item_week_grid g
left join weekly_scoped ws
on g.item_number = ws.item_number
and g.week_start = ws.week_start
),

rolling_metrics as (
select
f.item_number,
avg(case
when f.week_start >= dateadd(week, -4, m.current_week_start)
then f.weekly_units_sold
end) as trailing_4wk_avg_units,
avg(f.weekly_units_sold) as trailing_8wk_avg_units
from item_week_filled f
cross join max_date m
group by 1
),

baseline_forecast as (
select
r.item_number,
r.trailing_4wk_avg_units,
r.trailing_8wk_avg_units,
greatest(0, r.trailing_4wk_avg_units, r.trailing_8wk_avg_units) as selected_baseline_units,
round(greatest(0, r.trailing_4wk_avg_units, r.trailing_8wk_avg_units) * 1.2, 0) as recommended_ship_qty
from rolling_metrics r
)

select
b.item_number,
d.item_description,
d.category_name,
b.trailing_4wk_avg_units,
b.trailing_8wk_avg_units,
b.selected_baseline_units,
b.recommended_ship_qty::integer as recommended_ship_qty
from baseline_forecast b
left join {{ ref('dim_item') }} d
on b.item_number = d.item_number
order by b.recommended_ship_qty desc, b.item_number
