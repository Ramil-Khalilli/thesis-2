{{ config(materialized='table') }}

-- One row per known customer (rows without a customer ID are not included).

with lines as (
    select *
    from {{ ref('invoice_lines') }}
    where line_type = 'product' and customer_id is not null
)

select
    customer_id,
    arg_max(country, invoice_ts)                                         as country,          -- most recent
    count(distinct country)                                              as countries_used,
    min(invoice_date) filter (where not is_cancellation)                 as first_purchase_date,
    max(invoice_date) filter (where not is_cancellation)                 as last_purchase_date,
    count(distinct invoice) filter (where not is_cancellation)           as invoices,
    count(distinct invoice) filter (where is_cancellation)               as cancelled_invoices,
    sum(line_amount)                                                     as net_revenue,
    sum(quantity) filter (where not is_cancellation)                     as units_bought
from lines
group by customer_id
