# Experiment 2 – scalability

Average of 3 daily batches per volume. Ingest = landing -> bronze; transform = dbt build of silver + gold for that day, including all tests. Year 1 history stays at its real size.

| volume | rows per day | file MB | ingest s | transform s | total s | rows per second | time vs x1 |
|---|---|---|---|---|---|---|---|
| x1 | 2,473 | 0.2 | 0.24 | 7.39 | 7.62 | 324 | 1.0x |
| x10 | 24,730 | 2.3 | 0.26 | 7.13 | 7.39 | 3,345 | 1.0x |
| x100 | 247,300 | 23.4 | 1.11 | 9.11 | 10.21 | 24,213 | 1.3x |

## Per batch (with the three slowest dbt models)

| factor | day | rows | file_mb | ingest_s | transform_s | slowest_models |
|---|---|---|---|---|---|---|
| 1 | 2010-12-01 | 3108 | 0.3 | 0.23 | 8.2 | dim_product 0.3s, batch_quality 0.2s, invoice_lines 0.2s |
| 1 | 2010-12-02 | 2109 | 0.2 | 0.21 | 7.27 | dim_product 0.4s, dim_customer 0.2s, batch_quality 0.2s |
| 1 | 2010-12-03 | 2202 | 0.2 | 0.27 | 6.69 | dim_product 0.3s, batch_quality 0.2s, dim_customer 0.2s |
| 10 | 2010-12-01 | 31080 | 2.9 | 0.31 | 6.38 | dim_product 0.3s, invoice_lines 0.3s, batch_quality 0.2s |
| 10 | 2010-12-02 | 21090 | 2.1 | 0.25 | 8.7 | invoice_lines 1.4s, dim_product 0.4s, batch_quality 0.2s |
| 10 | 2010-12-03 | 22020 | 2.0 | 0.23 | 6.31 | dim_product 0.4s, invoice_lines 0.2s, batch_quality 0.2s |
| 100 | 2010-12-01 | 310800 | 29.3 | 1.32 | 10.26 | invoice_lines 1.3s, fact_sales 1.1s, batch_quality 0.7s |
| 100 | 2010-12-02 | 210900 | 20.6 | 0.95 | 8.68 | invoice_lines 0.9s, fact_sales 0.8s, dim_product 0.4s |
| 100 | 2010-12-03 | 220200 | 20.3 | 1.05 | 8.38 | invoice_lines 0.8s, fact_sales 0.6s, dim_product 0.5s |
