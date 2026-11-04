{{ config(materialized='view') }}

-- Batch-level checks: a file can be perfectly valid row by row and still be wrong as a whole
-- (half the day missing, customer IDs dropped by an upstream bug). Each batch is compared with the
-- median of the 28 batches before it. Anomalies are warnings: the data is loaded, and a person reviews it.

with batches as (
    select
        batch_date,
        rows_bronze,
        duplicates_dropped::double / nullif(rows_bronze, 0)   as duplicate_share,
        missing_customer_lines::double / nullif(rows_silver, 0) as missing_customer_share,
        median(rows_bronze) over previous                      as typical_rows,
        median(missing_customer_lines::double / nullif(rows_silver, 0)) over previous as typical_missing_share,
        count(*) over previous                                 as previous_batches
    from {{ ref('batch_quality') }}
    window previous as (order by batch_date rows between 28 preceding and 1 preceding)
)

select batch_date, 'volume_drop' as anomaly, rows_bronze::double as value, typical_rows as typical
from batches where previous_batches >= 10 and rows_bronze < 0.2 * typical_rows
union all
select batch_date, 'volume_spike', rows_bronze, typical_rows
from batches where previous_batches >= 10 and rows_bronze > 5 * typical_rows
union all
select batch_date, 'missing_customer_spike', missing_customer_share, typical_missing_share
from batches where previous_batches >= 10 and missing_customer_share > typical_missing_share + 0.4
union all
select batch_date, 'duplicate_spike', duplicate_share, null
from batches where duplicate_share > 0.10
