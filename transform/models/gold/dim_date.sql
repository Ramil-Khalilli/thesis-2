{{ config(materialized='table') }}

-- Calendar for the whole data period. is_trading_day = the shop had at least one product sale that day.

with days as (
    select cast(d as date) as date_day
    from generate_series(date '2009-12-01', date '2011-12-31', interval 1 day) as t(d)
),
trading as (
    select distinct invoice_date
    from {{ ref('invoice_lines') }}
    where line_type = 'product' and not is_cancellation
)

select
    days.date_day,
    year(date_day)                                              as year,
    quarter(date_day)                                           as quarter,
    month(date_day)                                             as month,
    monthname(date_day)                                         as month_name,
    weekofyear(date_day)                                        as iso_week,
    isodow(date_day)                                            as day_of_week,      -- 1 = Monday
    dayname(date_day)                                           as day_name,
    isodow(date_day) in (6, 7)                                  as is_weekend,
    h.holiday_date is not null                                  as is_uk_bank_holiday,
    h.holiday_name,
    t.invoice_date is not null                                  as is_trading_day,
    date_diff('day', date_day, make_date(year(date_day), 12, 25)) as days_to_christmas  -- negative after Christmas
from days
left join {{ ref('uk_bank_holidays') }} h on h.holiday_date = days.date_day
left join trading t on t.invoice_date = days.date_day
