with params as (
  select
    to_date('{month_start}') as month_start,
    {trend_years} as trend_years
),
bounds as (
  select
    month_start as m_start,
    dateadd(month, 1, month_start) as m_end,
    dateadd(year, -1, month_start) as prior_m_start,
    dateadd(year, -1, dateadd(month, 1, month_start)) as prior_m_end,
    dateadd(month, -2, month_start) as t3m_start,
    dateadd(month, 1, month_start) as t3m_end,
    dateadd(year, -1, dateadd(month, -2, month_start)) as prior_t3m_start,
    dateadd(year, -1, dateadd(month, 1, month_start)) as prior_t3m_end,
    dateadd(month, -11, month_start) as t12m_start,
    dateadd(month, 1, month_start) as t12m_end,
    dateadd(year, -1, dateadd(month, -11, month_start)) as prior_t12m_start,
    dateadd(year, -1, dateadd(month, 1, month_start)) as prior_t12m_end,
    year(month_start) - trend_years + 1 as cagr_start_year,
    year(month_start) as cagr_end_year,
    date_from_parts(year(month_start) - trend_years + 1, 1, 1) as cagr_start_year_start,
    date_from_parts(year(month_start) - trend_years + 2, 1, 1) as cagr_start_year_end,
    date_from_parts(year(month_start), 1, 1) as cagr_end_year_start,
    date_from_parts(year(month_start) + 1, 1, 1) as cagr_end_year_end
  from params
),
all_families as (
  select column1 as category_family
  from values
    ('Whiskies'),
    ('Tequilas'),
    ('Vodkas'),
    ('Brandies'),
    ('Rum'),
    ('Gin'),
    ('Liqueurs'),
    ('RTD/Cocktails'),
    ('Specialty/Other Spirits'),
    ('Other')
),
family_sales as (
  select
    d.category_family,
    sum(case when f.order_date >= b.m_start and f.order_date < b.m_end then f.sale_dollars else 0 end) as sales_month,
    sum(case when f.order_date >= b.prior_m_start and f.order_date < b.prior_m_end then f.sale_dollars else 0 end) as sales_prior_month,
    sum(case when f.order_date >= b.t3m_start and f.order_date < b.t3m_end then f.sale_dollars else 0 end) as sales_t3m,
    sum(case when f.order_date >= b.prior_t3m_start and f.order_date < b.prior_t3m_end then f.sale_dollars else 0 end) as sales_prior_t3m,
    sum(case when f.order_date >= b.t12m_start and f.order_date < b.t12m_end then f.sale_dollars else 0 end) as sales_t12m,
    sum(case when f.order_date >= b.prior_t12m_start and f.order_date < b.prior_t12m_end then f.sale_dollars else 0 end) as sales_prior_t12m,
    sum(case when f.order_date >= b.cagr_start_year_start and f.order_date < b.cagr_start_year_end then f.sale_dollars else 0 end) as sales_cagr_start_year,
    sum(case when f.order_date >= b.cagr_end_year_start and f.order_date < b.cagr_end_year_end then f.sale_dollars else 0 end) as sales_cagr_end_year,
    max(b.cagr_start_year) as cagr_start_year,
    max(b.cagr_end_year) as cagr_end_year
  from planning_os.dev.fct_liquor_sales f
  join planning_os.dev.dim_item d
    on f.item_number = d.item_number
  cross join bounds b
  group by 1
),
detail_rows as (
  select
    a.category_family,
    coalesce(s.sales_month, 0) as sales_month,
    coalesce(s.sales_prior_month, 0) as sales_prior_month,
    case
      when coalesce(s.sales_prior_month, 0) = 0 then null
      else round(((coalesce(s.sales_month, 0) - s.sales_prior_month) / s.sales_prior_month) * 100, 2)
    end as month_yoy_pct,
    coalesce(s.sales_t3m, 0) as sales_t3m,
    coalesce(s.sales_prior_t3m, 0) as sales_prior_t3m,
    case
      when coalesce(s.sales_prior_t3m, 0) = 0 then null
      else round(((coalesce(s.sales_t3m, 0) - s.sales_prior_t3m) / s.sales_prior_t3m) * 100, 2)
    end as t3m_yoy_pct,
    coalesce(s.sales_t12m, 0) as sales_t12m,
    coalesce(s.sales_prior_t12m, 0) as sales_prior_t12m,
    case
      when coalesce(s.sales_prior_t12m, 0) = 0 then null
      else round(((coalesce(s.sales_t12m, 0) - s.sales_prior_t12m) / s.sales_prior_t12m) * 100, 2)
    end as t12m_yoy_pct,
    coalesce(s.sales_cagr_start_year, 0) as sales_cagr_start_year,
    coalesce(s.sales_cagr_end_year, 0) as sales_cagr_end_year,
    s.cagr_start_year,
    s.cagr_end_year
  from all_families a
  left join family_sales s
    on a.category_family = s.category_family
)
select
  category_family,
  sales_month,
  sales_prior_month,
  month_yoy_pct,
  sales_t3m,
  sales_prior_t3m,
  t3m_yoy_pct,
  sales_t12m,
  sales_prior_t12m,
  t12m_yoy_pct,
  sales_cagr_start_year,
  sales_cagr_end_year,
  cagr_start_year,
  cagr_end_year
from detail_rows

union all

select
  'Grand Total' as category_family,
  sum(sales_month) as sales_month,
  sum(sales_prior_month) as sales_prior_month,
  case
    when sum(sales_prior_month) = 0 then null
    else round(((sum(sales_month) - sum(sales_prior_month)) / sum(sales_prior_month)) * 100, 2)
  end as month_yoy_pct,
  sum(sales_t3m) as sales_t3m,
  sum(sales_prior_t3m) as sales_prior_t3m,
  case
    when sum(sales_prior_t3m) = 0 then null
    else round(((sum(sales_t3m) - sum(sales_prior_t3m)) / sum(sales_prior_t3m)) * 100, 2)
  end as t3m_yoy_pct,
  sum(sales_t12m) as sales_t12m,
  sum(sales_prior_t12m) as sales_prior_t12m,
  case
    when sum(sales_prior_t12m) = 0 then null
    else round(((sum(sales_t12m) - sum(sales_prior_t12m)) / sum(sales_prior_t12m)) * 100, 2)
  end as t12m_yoy_pct,
  sum(sales_cagr_start_year) as sales_cagr_start_year,
  sum(sales_cagr_end_year) as sales_cagr_end_year,
  max(cagr_start_year) as cagr_start_year,
  max(cagr_end_year) as cagr_end_year
from detail_rows

order by
  case when category_family = 'Grand Total' then 1 else 0 end,
  sales_t12m desc nulls last,
  category_family;
