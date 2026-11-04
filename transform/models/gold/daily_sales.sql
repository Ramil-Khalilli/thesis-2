{{ config(
    materialized='incremental',
    incremental_strategy='append',
    pre_hook=["{{ replace_invoice_dates_hook() }}"]
) }}

-- Company-level KPIs per invoice date. This is the table the revenue forecast is built on.
-- Only the invoice dates touched by this run's batches are recomputed (from all of silver).

with affected_dates as (
    select distinct invoice_date from {{ ref('invoice_lines') }} where {{ batch_filter() }}
),

lines as (
    select *
    from {{ ref('invoice_lines') }}
    where invoice_date in (select invoice_date from affected_dates)
      and line_type in ('product', 'postage')
),

first_purchase as (
    select customer_id, min(invoice_date) as first_purchase_date
    from {{ ref('invoice_lines') }}
    where line_type = 'product' and not is_cancellation and customer_id is not null
    group by customer_id
),

daily as (
    select
        l.invoice_date,
        sum(line_amount) filter (where line_type = 'product' and not is_cancellation)          as gross_product_revenue,
        coalesce(sum(line_amount) filter (where line_type = 'product' and is_cancellation), 0) as cancelled_revenue,
        sum(line_amount) filter (where line_type = 'product')                                  as net_product_revenue,
        coalesce(sum(line_amount) filter (where line_type = 'postage'), 0)                     as postage_revenue,
        count(distinct invoice) filter (where line_type = 'product' and not is_cancellation)   as invoices,
        count(distinct invoice) filter (where is_cancellation)                                 as cancelled_invoices,
        count(distinct l.customer_id) filter (where line_type = 'product' and not is_cancellation) as customers,
        count(distinct l.customer_id) filter (where fp.first_purchase_date = l.invoice_date)   as new_customers,
        count(*) filter (where line_type = 'product' and not is_cancellation)                  as sale_lines,
        sum(quantity) filter (where line_type = 'product' and not is_cancellation)             as units_sold,
        sum(line_amount) filter (where line_type = 'product' and not is_cancellation
                                   and country <> 'United Kingdom')                           as non_uk_revenue,
        count(*) filter (where line_type = 'product' and not is_cancellation
                           and is_missing_customer)                                            as lines_without_customer
    from lines l
    left join first_purchase fp using (customer_id)
    group by l.invoice_date
)

select
    *,
    round(gross_product_revenue / nullif(invoices, 0), 2)                 as avg_invoice_value,
    round(coalesce(non_uk_revenue, 0) / nullif(gross_product_revenue, 0), 4) as non_uk_share,
    round(-cancelled_revenue / nullif(gross_product_revenue, 0), 4)        as cancellation_rate,
    now()::timestamp                                                     as _gold_loaded_at
from daily
