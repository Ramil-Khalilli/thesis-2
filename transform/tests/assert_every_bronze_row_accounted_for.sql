-- Reconciliation: for every batch, bronze rows = duplicates dropped + quarantined rows + silver rows.
-- Returns the batches where this does not hold (the test fails if any row is returned).

with bronze as (
    select batch_date, count(*) as n from {{ source('bronze', 'invoice_lines') }} group by 1
),
silver as (
    select batch_date, count(*) as n from {{ ref('invoice_lines') }} group by 1
),
quarantine as (
    select batch_date, count(*) as n from {{ ref('quarantine_lines') }} group by 1
),
quality as (
    select batch_date, duplicates_dropped from {{ ref('batch_quality') }}
)

select
    b.batch_date,
    b.n                              as bronze_rows,
    coalesce(s.n, 0)                 as silver_rows,
    coalesce(qu.n, 0)                as quarantined_rows,
    coalesce(q.duplicates_dropped, 0) as duplicates_dropped
from bronze b
left join silver s      using (batch_date)
left join quarantine qu using (batch_date)
left join quality q     using (batch_date)
where b.n <> coalesce(s.n, 0) + coalesce(qu.n, 0) + coalesce(q.duplicates_dropped, 0)
