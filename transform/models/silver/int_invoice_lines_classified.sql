{{ config(materialized='ephemeral') }}

-- Shared logic for all silver tables: deduplicate, type, validate and classify the bronze rows
-- of the batches being processed. Rules come from the audit in Data_Exploration.ipynb.

with src as (
    select *
    from {{ source('bronze', 'invoice_lines') }}
    where {{ batch_filter() }}
),

-- Rule 2: exact duplicate lines within a batch -> keep the first copy only.
dedup as (
    select
        *,
        row_number() over (
            partition by batch_date, "Invoice", "StockCode", "Description", "Quantity",
                         "InvoiceDate", "Price", "Customer ID", "Country"
            order by _row_number
        ) as copy_number
    from src
),

typed as (
    select
        batch_date,
        trim("Invoice")                                               as invoice,
        case when regexp_matches(trim("Invoice"), '^[A-Za-z]')
             then upper(left(trim("Invoice"), 1)) else '' end         as invoice_prefix,
        upper(trim("StockCode"))                                      as stock_code,
        nullif(trim("Description"), '')                               as description,
        try_cast("Quantity" as integer)                               as quantity,
        try_cast("InvoiceDate" as timestamp)                          as invoice_ts,
        try_cast("Price" as decimal(12, 3))                           as unit_price,
        nullif(regexp_replace(trim("Customer ID"), '\.0$', ''), '')   as customer_id,
        nullif(trim("Country"), '')                                   as country,
        "Quantity"                                                    as raw_quantity,
        "InvoiceDate"                                                 as raw_invoice_date,
        "Price"                                                       as raw_price,
        copy_number > 1                                               as is_duplicate,
        _source_file,
        _file_hash,
        _row_number
    from dedup
),

classified as (
    select
        *,
        cast(invoice_ts as date)  as invoice_date,
        invoice_prefix = 'C'      as is_cancellation,

        -- Row-level validation: the first failing check is the quarantine reason (null = valid).
        case
            when invoice is null or invoice = ''                         then 'missing_invoice'
            when stock_code is null or stock_code = ''                   then 'missing_stock_code'
            when quantity is null                                        then 'invalid_quantity'
            when unit_price is null                                      then 'invalid_price'
            when invoice_ts is null                                      then 'invalid_timestamp'
            when cast(invoice_ts as date) > batch_date                   then 'future_date'
            when starts_with(stock_code, 'TEST')                         then 'test_product'
            when unit_price < 0 and invoice_prefix <> 'A'                then 'negative_price'
            when invoice_prefix = 'C' and quantity > 0                   then 'cancellation_with_positive_quantity'
        end as dq_reason,

        -- Rules 4, 5 and 8: what kind of line this is.
        case
            when invoice_prefix = 'A' or stock_code = 'B'                 then 'adjustment'
            when stock_code in ('POST', 'DOT', 'C2')                      then 'postage'
            when stock_code = 'M' or starts_with(stock_code, 'ADJUST')    then 'manual'
            when stock_code = 'D'                                         then 'discount'
            when stock_code = 'S'                                         then 'sample'
            when stock_code in ('BANK CHARGES', 'AMAZONFEE', 'CRUK')      then 'fee'
            when starts_with(stock_code, 'GIFT_')                         then 'voucher'
            when regexp_matches(stock_code, '^[0-9]{5}[A-Z]{0,2}$')
              or starts_with(stock_code, 'DCGS') or stock_code in ('PADS', 'SP1002') then
                case when invoice_prefix <> 'C' and quantity <= 0
                     then 'stock_correction' else 'product' end
            else 'other'
        end as line_type
    from typed
)

select
    *,
    quantity * unit_price                                                    as line_amount,
    customer_id is null                                                      as is_missing_customer,
    line_type = 'product' and unit_price = 0 and not is_cancellation         as is_free_item,
    abs(quantity) > {{ var('extreme_quantity') }}                            as is_extreme_quantity,
    invoice_date < batch_date                                                as is_late_arrival
from classified
