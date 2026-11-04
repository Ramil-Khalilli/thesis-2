{{ config(
    materialized='incremental',
    incremental_strategy='append',
    pre_hook=["{{ replace_batches_hook() }}"]
) }}

-- One row per batch: where every bronze row went and how many rows carry each data issue.
-- rows_bronze = duplicates_dropped + rows_quarantined + rows_silver (checked by a test).

select
    batch_date,
    count(*)                                                                    as rows_bronze,
    count(*) filter (where is_duplicate)                                        as duplicates_dropped,
    count(*) filter (where not is_duplicate and dq_reason is not null)          as rows_quarantined,
    count(*) filter (where not is_duplicate and dq_reason is null)              as rows_silver,
    count(*) filter (where not is_duplicate and dq_reason is null
                     and line_type = 'product' and not is_cancellation)         as product_sale_lines,
    count(*) filter (where not is_duplicate and dq_reason is null
                     and is_cancellation)                                       as cancellation_lines,
    count(*) filter (where not is_duplicate and dq_reason is null
                     and line_type = 'stock_correction')                        as stock_correction_lines,
    count(*) filter (where not is_duplicate and dq_reason is null
                     and line_type not in ('product', 'stock_correction'))      as non_product_lines,
    count(*) filter (where not is_duplicate and dq_reason is null
                     and line_type = 'other')                                   as unknown_code_lines,
    count(*) filter (where not is_duplicate and dq_reason is null
                     and is_missing_customer)                                   as missing_customer_lines,
    count(*) filter (where not is_duplicate and dq_reason is null
                     and is_free_item)                                          as free_item_lines,
    count(*) filter (where not is_duplicate and dq_reason is null
                     and is_extreme_quantity)                                   as extreme_quantity_lines,
    count(*) filter (where not is_duplicate and dq_reason is null
                     and is_late_arrival)                                       as late_arrival_lines,
    max(_file_hash)                                                             as source_file_hash,
    now()::timestamp                                                            as checked_at
from {{ ref('int_invoice_lines_classified') }}
group by batch_date
