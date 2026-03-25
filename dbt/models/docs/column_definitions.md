{% docs invoice_item_number %}
Unique invoice line identifier at the atomic transaction grain.
Primary business key for `fct_liquor_sales`.
{% enddocs %}

{% docs store_number %}
Business key for store identity.
Used to join facts to `dim_store` and `snap_store`.
{% enddocs %}

{% docs item_number %}
Business key for item identity.
Used to join facts to `dim_item` and `snap_item`.
{% enddocs %}

{% docs order_date %}
Transaction date associated with the invoice line.
Used for daily and period-based aggregation.
{% enddocs %}

{% docs sale_dollars %}
Revenue amount in USD for the transaction row.
In aggregated models, this is summed to total sales.
{% enddocs %}

{% docs bottles_sold %}
Number of bottles sold for the transaction row.
In aggregated models, this is summed by grain.
{% enddocs %}

{% docs loaded_at %}
Ingestion timestamp recorded when the source row was loaded into the raw layer.
Used for freshness and incremental boundaries.
{% enddocs %}

{% docs dbt_valid_from %}
Snapshot effective-start timestamp for a historical version of the row.
{% enddocs %}

{% docs dbt_valid_to %}
Snapshot effective-end timestamp for a historical version of the row.
`null` indicates the current active version.
{% enddocs %}