{{ config(
    materialized='incremental',
    incremental_strategy='append',
    pre_hook=["{{ replace_batches_hook() }}"]
) }}

-- One row per product sale or cancellation line. Postage, fees, adjustments and stock corrections are
-- left out (they stay in silver). Cancellations have negative quantity and amount, so sums give net values.

with lines as (
    select *
    from {{ ref('invoice_lines') }}
    where {{ batch_filter() }}
      and line_type = 'product'
),

-- Earlier sales, used to check whether a cancellation reverses a sale we know about.
past_sales as (
    select customer_id, stock_code, quantity, invoice_ts
    from {{ ref('invoice_lines') }}
    where line_type = 'product' and not is_cancellation and customer_id is not null
)

select
    md5(l._file_hash || '|' || l._row_number)          as sales_line_key,
    l.batch_date,
    l.invoice,
    l.invoice_ts,
    l.invoice_date,
    l.stock_code,
    p.product_version_key,
    l.customer_id,
    l.country,
    l.quantity,
    l.unit_price,
    l.line_amount,
    l.is_cancellation,
    case when l.is_cancellation then exists (
        select 1 from past_sales s
        where s.customer_id = l.customer_id
          and s.stock_code  = l.stock_code
          and s.quantity    = -l.quantity
          and s.invoice_ts <= l.invoice_ts
    ) end                                              as has_matching_sale,
    l.is_free_item,
    l.is_extreme_quantity,
    l.is_missing_customer,
    now()::timestamp                                   as _gold_loaded_at
from lines l
left join {{ ref('dim_product') }} p
       on p.stock_code = l.stock_code
      and l.invoice_date >= p.valid_from
      and l.invoice_date <  p.valid_to
