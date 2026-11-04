{{ config(severity='warn') }}

-- Warning (not an error): stock codes that fit no known pattern. They are loaded as line_type 'other'
-- and should be reviewed, then added to the classification in int_invoice_lines_classified.sql.

select stock_code, min(description) as example_description, count(*) as lines
from {{ ref('invoice_lines') }}
where line_type = 'other'
group by stock_code
