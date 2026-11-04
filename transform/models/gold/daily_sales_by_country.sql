{{ config(
    materialized='incremental',
    incremental_strategy='append',
    pre_hook=["{{ replace_invoice_dates_hook() }}"]
) }}

-- Net product revenue per invoice date and country (for dashboards and a possible per-country forecast).

with affected_dates as (
    select distinct invoice_date from {{ ref('invoice_lines') }} where {{ batch_filter() }}
)

select
    invoice_date,
    country,
    sum(line_amount)                                                as net_product_revenue,
    count(distinct invoice) filter (where not is_cancellation)      as invoices,
    count(distinct customer_id) filter (where not is_cancellation)  as customers,
    now()::timestamp                                                as _gold_loaded_at
from {{ ref('invoice_lines') }}
where invoice_date in (select invoice_date from affected_dates)
  and line_type = 'product'
group by invoice_date, country
