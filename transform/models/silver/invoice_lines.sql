{{ config(
    materialized='incremental',
    incremental_strategy='append',
    pre_hook=["{{ replace_batches_hook() }}"]
) }}

-- Clean, typed invoice lines: every valid, non-duplicate bronze row of the processed batches.
-- Nothing is summed or removed here except duplicates and invalid rows; the flags let gold decide.

select
    batch_date,
    invoice,
    invoice_prefix,
    is_cancellation,
    stock_code,
    description,
    quantity,
    unit_price,
    line_amount,
    invoice_ts,
    invoice_date,
    customer_id,
    country,
    line_type,
    is_missing_customer,
    is_free_item,
    is_extreme_quantity,
    is_late_arrival,
    _source_file,
    _file_hash,
    _row_number,
    now()::timestamp as _silver_loaded_at
from {{ ref('int_invoice_lines_classified') }}
where not is_duplicate
  and dq_reason is null
