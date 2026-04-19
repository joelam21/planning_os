{% docs sales %}
Canonical revenue metric in USD.

Default definition:
`sum(sale_dollars)`

Usage notes:
- In atomic facts, `sale_dollars` is the invoice-line revenue amount.
- In aggregated models and notebooks, `sales` should mean the sum of `sale_dollars` at the target grain.
- Because the source is wholesale sell-in data, this represents store purchases from the state, not consumer sell-through.
{% enddocs %}

{% docs units %}
Canonical unit-volume metric.

Default definition:
`sum(bottles_sold)`

Usage notes:
- `bottles_sold` is the base transactional unit in the source data.
- In aggregated models and notebooks, `units` should mean the sum of `bottles_sold` at the target grain.
- Return-aware interpretation matters: negative values should only occur on legitimate RINV rows.
{% enddocs %}

{% docs avg_selling_price %}
Canonical average selling price metric.

Default definition:
`sum(sale_dollars) / nullif(sum(bottles_sold), 0)`

Usage notes:
- This is a weighted average price per bottle, not a simple average of row-level prices.
- ASP should be interpreted carefully for channels or vendors with a heavy mix of non-standard sizes, bundle packs, or trial formats.
- In this repo, ASP is primarily used for comparative analysis rather than pricing policy enforcement.
{% enddocs %}

{% docs gross_profit %}
Estimated gross profit metric.

Default definition:
`sum(sale_dollars - (state_bottle_cost * bottles_sold))`

Usage notes:
- This is an estimated gross profit proxy derived from state bottle cost and sale dollars.
- It does not include retailer operating costs, promotions, spoilage, or non-product margin adjustments.
- Best used for directional comparison, not exact financial reporting.
{% enddocs %}

{% docs share_pct %}
Canonical share-of-total metric expressed as a percentage.

Default definition:
`metric_value / nullif(total_metric_value, 0)`

Usage notes:
- Share should always be defined relative to an explicit comparison set and time window.
- In this repo, the most common usage is vendor share within a category family over a trailing 12-month window.
- Share comparisons are only meaningful when numerator and denominator use the same metric and grain.
{% enddocs %}

{% docs yoy_growth_pct %}
Canonical year-over-year growth rate metric.

Default definition:
`(current_period_value / nullif(prior_period_value, 0)) - 1`

Usage notes:
- Use the same metric, grain, and comparable window for current and prior values.
- In this repo, YoY is commonly used on trailing 12-month sales windows.
- When prior-period values are near zero, the result may be unstable and should be interpreted cautiously.
{% enddocs %}

{% docs cagr %}
Canonical compound annual growth rate metric.

Default definition:
`(ending_value / nullif(beginning_value, 0)) ^ (1 / year_span) - 1`

Usage notes:
- CAGR should be calculated from comparable annual totals, not from single endpoint months, unless explicitly stated otherwise.
- In this repo, CAGR is used to compare multi-year vendor and category performance across a defined fiscal or calendar window.
- Always document the start year, end year, and interval length when presenting CAGR.
{% enddocs %}

{% docs vendor_share %}
Vendor share metric within an explicitly defined market.

Default definition:
`vendor_sales / nullif(total_category_sales, 0)`

Usage notes:
- Vendor share should always state the category scope, time window, and metric basis.
- In this repo, vendor share is primarily evaluated within category-family trailing 12-month windows.
- Vendor rank changes are best interpreted alongside growth rates and price/channel mix, not as standalone signals.
{% enddocs %}

{% docs sku_velocity_tier %}
Pareto-style SKU productivity classification used in the SKU rationalization analysis.

Default definition:
- Tier 1: top 80% of cumulative contribution
- Tier 2: next 15%
- Tier 3: bottom 5%

Usage notes:
- Volume tier and revenue tier are calculated independently.
- Off-diagonal cases are analytically important because they reveal divergence between operational importance and commercial importance.
- Tier membership is a decision-support heuristic, not a universal assortment rule.
{% enddocs %}

{% docs weekly_units_sold %}
Weekly unit-demand metric at the item-by-week grain.

Default definition:
`sum(bottles_sold)` aggregated to `date_trunc('week', order_date), item_number`

Usage notes:
- This metric is zero-filled across the historical week spine before rolling averages are computed.
- Return-labeled invoice rows are excluded from replenishment demand calculations in this repo.
- It is intended for replenishment planning, not for financial reporting.
{% enddocs %}

{% docs trailing_avg_units %}
Trailing average weekly unit-demand metric.

Default definition:
`avg(weekly_units_sold)` across a defined trailing week window

Usage notes:
- In this repo, the primary windows are trailing 4 full weeks and trailing 8 full weeks.
- The averages are computed from a zero-filled weekly item history so intermittent-demand items are not overstated.
- These metrics are decision-support baselines, not forecast accuracy guarantees.
{% enddocs %}

{% docs selected_baseline_units %}
Chosen replenishment demand baseline expressed in units.

Default definition:
`greatest(0, trailing_4wk_avg_units, trailing_8wk_avg_units)`

Usage notes:
- The model intentionally selects the stronger recent baseline rather than averaging the two windows together.
- Flooring at zero prevents negative or nonsensical replenishment recommendations.
- This metric is an intermediate planning baseline before any safety buffer is applied.
{% enddocs %}

{% docs recommended_ship_qty %}
Recommended replenishment shipment quantity in units.

Default definition:
`round(selected_baseline_units * 1.2, 0)`

Usage notes:
- In this repo, the default policy applies a fixed 20% safety buffer over the selected baseline.
- The result is rounded to a whole-unit shipment recommendation.
- This is a heuristic planning output and should be interpreted alongside category context and operational constraints.
{% enddocs %}
