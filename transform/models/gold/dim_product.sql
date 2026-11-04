{{ config(materialized='table') }}

-- Product dimension with SCD Type 2 history of the description.
-- One row per version of a product: a new version starts on the first day a new description becomes
-- the product's most common one. valid_from is inclusive, valid_to exclusive; the current version has valid_to 9999-12-31.
-- Price is not tracked here: it varies by order size and belongs to the sales fact.

with product_lines as (
    select stock_code, invoice_date, description, quantity, unit_price, is_cancellation
    from {{ ref('invoice_lines') }}
    where line_type = 'product'
),

-- The description used most on each day (ties broken alphabetically, so the result is repeatable).
daily_description as (
    select stock_code, invoice_date, description
    from (
        select
            stock_code, invoice_date, description,
            row_number() over (partition by stock_code, invoice_date
                               order by count(*) desc, description) as rank
        from product_lines
        where description is not null
        group by stock_code, invoice_date, description
    )
    where rank = 1
),

-- A new version starts when a product gets a description it never had before. Going back to an older
-- name (common while the shop's catalogue was being renamed) does not create a version, so the history
-- shows real renames instead of day-to-day back-and-forth.
first_seen as (
    select stock_code, description, min(invoice_date) as first_day
    from daily_description
    group by stock_code, description
),

versions as (
    select
        stock_code, description, first_day,
        row_number() over (partition by stock_code order by first_day, description) as version_number
    from first_seen
),

-- Products that never had a description still get one (unknown) version, so every sale finds a product.
all_products as (
    select distinct stock_code from product_lines
),

with_unknown as (
    select a.stock_code, coalesce(v.version_number, 1) as version_number, v.description, v.first_day
    from all_products a
    left join versions v using (stock_code)
),

-- Most common sale quantity. mode() picks arbitrarily between equally common values, which made
-- rebuilds differ (found by the fault-injection experiment), so ties go to the smaller quantity.
pack_size as (
    select stock_code, quantity as typical_pack_size
    from (
        select stock_code, quantity,
               row_number() over (partition by stock_code order by count(*) desc, quantity) as rank
        from product_lines
        where not is_cancellation and quantity > 0
        group by stock_code, quantity
    )
    where rank = 1
),

attributes as (
    select
        stock_code,
        median(unit_price) filter (where not is_cancellation and unit_price > 0)      as median_unit_price,
        min(invoice_date) filter (where not is_cancellation)                         as first_sold_date,
        max(invoice_date) filter (where not is_cancellation)                         as last_sold_date
    from product_lines
    group by stock_code
)

select
    md5(w.stock_code || '|' || w.version_number)                               as product_version_key,
    w.stock_code,
    w.version_number,
    coalesce(w.description, '(no description)')                                as description,
    -- The first version is valid from the start, so sales before the first described day still match it.
    case when w.version_number = 1 then date '1900-01-01' else w.first_day end as valid_from,
    coalesce(lead(w.first_day) over (partition by w.stock_code order by w.version_number),
             date '9999-12-31')                                                as valid_to,
    lead(w.first_day) over (partition by w.stock_code order by w.version_number) is null as is_current,
    ps.typical_pack_size,
    a.median_unit_price,
    a.first_sold_date,
    a.last_sold_date
from with_unknown w
left join attributes a using (stock_code)
left join pack_size ps using (stock_code)
