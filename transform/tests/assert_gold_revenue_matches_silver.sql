-- Reconciliation: net product revenue in gold (daily_sales, daily_sales_by_country, fact_sales)
-- must equal the sum of product line amounts in silver. Returns a row if any total differs.

with totals as (
    select
        (select sum(line_amount) from {{ ref('invoice_lines') }} where line_type = 'product') as silver_total,
        (select sum(net_product_revenue) from {{ ref('daily_sales') }})                     as daily_total,
        (select sum(net_product_revenue) from {{ ref('daily_sales_by_country') }})          as country_total,
        (select sum(line_amount) from {{ ref('fact_sales') }})                              as fact_total
)

select *
from totals
where abs(silver_total - daily_total)   > 0.01
   or abs(silver_total - country_total) > 0.01
   or abs(silver_total - fact_total)    > 0.01
