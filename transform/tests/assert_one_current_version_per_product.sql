-- SCD Type 2 integrity: every product has exactly one current version, and its versions follow each
-- other without gaps or overlaps (each valid_to equals the next version's valid_from).

with checks as (
    select
        stock_code,
        count(*) filter (where is_current) as current_versions,
        count(*) filter (where valid_to <> coalesce(next_from, date '9999-12-31')) as broken_links
    from (
        select *, lead(valid_from) over (partition by stock_code order by version_number) as next_from
        from {{ ref('dim_product') }}
    )
    group by stock_code
)

select * from checks where current_versions <> 1 or broken_links > 0
