{# Use the schema name as written (silver, gold) instead of dbt's default "main_silver". #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}


{# SQL condition selecting the batch dates of this run (var batch_dates); all dates if none given. #}
{% macro batch_filter(column='batch_date') -%}
    {%- set dates = var('batch_dates', []) -%}
    {%- if dates is string -%}{%- set dates = [dates] -%}{%- endif -%}
    {%- if dates | length > 0 -%}
        {{ column }} in ({% for d in dates %}date '{{ d }}'{% if not loop.last %}, {% endif %}{% endfor %})
    {%- else -%}
        true
    {%- endif -%}
{%- endmacro %}


{# For gold tables keyed by invoice_date: recompute every invoice date that appears in the batches of this
   run. A late or corrected batch therefore also updates the KPIs of the earlier day it belongs to. #}
{% macro replace_invoice_dates_hook() -%}
    {%- if is_incremental() -%}
        delete from {{ this }} where invoice_date in (
            select distinct invoice_date from {{ ref('invoice_lines') }} where {{ batch_filter() }}
        )
    {%- else -%}
        select 1
    {%- endif -%}
{%- endmacro %}


{# Incremental pattern used by every silver table: remove the batches being reprocessed, then append them again.
   Re-running a day therefore replaces it and never doubles it. #}
{% macro replace_batches_hook() -%}
    {%- if is_incremental() -%}
        delete from {{ this }} where {{ batch_filter() }}
    {%- else -%}
        select 1
    {%- endif -%}
{%- endmacro %}
