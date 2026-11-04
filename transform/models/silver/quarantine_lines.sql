{{ config(
    materialized='incremental',
    incremental_strategy='append',
    pre_hook=["{{ replace_batches_hook() }}"]
) }}

-- Rows that failed validation, with the reason and the original text values for review.

select
    batch_date,
    dq_reason,
    invoice,
    stock_code,
    description,
    raw_quantity,
    raw_price,
    raw_invoice_date,
    customer_id,
    country,
    _source_file,
    _file_hash,
    _row_number,
    now()::timestamp as _silver_loaded_at
from {{ ref('int_invoice_lines_classified') }}
where not is_duplicate
  and dq_reason is not null
