{{ config(severity='warn') }}

-- Warning (not an error) for batches whose volume, missing customer IDs or duplicates look unusual.
-- Only batches processed in this run are reported, so old anomalies do not repeat on every build.

select * from {{ ref('batch_anomalies') }} where {{ batch_filter() }}
