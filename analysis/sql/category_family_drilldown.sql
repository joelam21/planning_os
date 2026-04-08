/*
  category_family_drilldown.sql
  -------------------------------------------------------
  Purpose:
    Calculates trailing-12-month sales and prior-year trailing-12-month sales by
    category_name within a selected category_family, with YoY percentage change and
    a grand total row for context.

  Parameters:
    month_start: Anchor month (YYYY-MM-01) used to build current and prior T12M windows.
    category_family: Category family filter for the category-level drilldown.
    database: Database containing marts and dimensions referenced in the query.
    schema: Schema containing fct_liquor_sales and dim_item.

  Returns:
    Grain is category_name within the selected family plus one grand total row.
    Key columns include sales_t12m, sales_prior_t12m, t12m_yoy_pct, and row_type.

  Used by:
    notebooks/01_category_growth_analysis.ipynb
*/

with params as (
  select
    to_date('{month_start}') as month_start,
    '{category_family}'      as category_family
),
bounds as (
  select
    dateadd(month, -11, month_start)                              as t12m_start,
    dateadd(month,   1, month_start)                              as t12m_end,
    dateadd(year, -1, dateadd(month, -11, month_start))           as prior_t12m_start,
    dateadd(year, -1, dateadd(month,   1, month_start))           as prior_t12m_end
  from params
),
category_sales as (
  select
    d.category_name,
    sum(case when f.order_date >= b.t12m_start       and f.order_date < b.t12m_end
             then f.sale_dollars else 0 end)          as sales_t12m,
    sum(case when f.order_date >= b.prior_t12m_start and f.order_date < b.prior_t12m_end
             then f.sale_dollars else 0 end)          as sales_prior_t12m
  from {database}.{schema}.fct_liquor_sales f
  join {database}.{schema}.dim_item d
    on f.item_number = d.item_number
  cross join params  p
  cross join bounds  b
  where d.category_family = p.category_family
  group by 1
),
detail_rows as (
  select
    category_name,
    sales_t12m,
    sales_prior_t12m,
    case
      when sales_prior_t12m = 0 then null
      else round(((sales_t12m - sales_prior_t12m) / sales_prior_t12m) * 100, 2)
    end as t12m_yoy_pct,
    0 as row_type
  from category_sales
)

select * from detail_rows

union all

select
  'Grand Total'          as category_name,
  sum(sales_t12m)        as sales_t12m,
  sum(sales_prior_t12m)  as sales_prior_t12m,
  case
    when sum(sales_prior_t12m) = 0 then null
    else round(((sum(sales_t12m) - sum(sales_prior_t12m)) / sum(sales_prior_t12m)) * 100, 2)
  end                    as t12m_yoy_pct,
  1                      as row_type
from detail_rows

order by row_type, sales_t12m desc nulls last
