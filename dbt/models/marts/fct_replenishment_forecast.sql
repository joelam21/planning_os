with replenishment_forecast as (
	select
		baseline.item_number,
		dim_item.item_description,
		dim_item.category_name,
		baseline.trailing_4wk_avg_units,
		baseline.trailing_8wk_avg_units,
		baseline.selected_baseline_units,
		baseline.recommended_ship_qty::integer as recommended_ship_qty
	from {{ ref('int_replenishment_baseline_forecast') }} as baseline
	left join {{ ref('dim_item') }} as dim_item
		on baseline.item_number = dim_item.item_number
)

select *
from replenishment_forecast
order by recommended_ship_qty desc, item_number
