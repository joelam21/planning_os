# Project Start Notes

- Grain:
	- Atomic fact grain is `invoice_item_number` in `fct_liquor_sales`.
	- Aggregated grains include store-day (`fct_store_daily_sales`) and SKU-level trailing windows (`fct_sku_velocity`).

- Facts:
	- `fct_liquor_sales` is the base transactional fact and preserves source signs for returns.
	- `fct_sku_velocity` excludes RINV rows so ranking reflects forward sales behavior.

- Dimensions:
	- `dim_store` and `dim_item` are Type 1 current-state dimensions sourced from Type 2 snapshots.

- Drift rules:
	- Non-RINV rows must not contain negative sales/volume metrics.
	- Positive RINV anomalies are monitored via `int_anomalous_returns` and reviewed periodically.
	- `assert_retail_gte_cost` remains warn-only and is tracked with threshold-based monitoring.

- Current roadmap focus:
	- Maintain and extend historical coverage beyond 2025 as new source data becomes available.
	- Keep enrichment-first development (chain categorization, SKU planning tiers).
	- Add scheduling and alerting after monitoring and run cadence are consistently stable.

