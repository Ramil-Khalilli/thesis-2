# Experiment 1 – fault injection

**31 of 31 checks passed** (9 file, 14 row, 5 batch, 3 reliability).

| fault | level | injected | expected | observed | result |
|---|---|---|---|---|---|
| renamed column (Price -> UnitPrice) | file | 1 | rejected | rejected: missing required columns: ['Price'] | PASS |
| file not UTF-8 encoded | file | 1 | rejected | rejected: file cannot be read as UTF-8 CSV: 'utf-8' codec can't decode byte 0xa3 in position 114: invalid start byte | PASS |
| file truncated mid-row | file | 1 | rejected | rejected: malformed file: file does not end with a line break (possibly truncated) | PASS |
| header only, no data rows | file | 1 | rejected | rejected: file has a header but no data rows | PASS |
| file name is not a date | file | 1 | rejected | rejected: file name is not YYYY-MM-DD.csv | PASS |
| unexpected extra column | file | 1 | loaded (column noted) | loaded: extra columns kept: ['Discount'] | PASS |
| corrected re-delivery of a loaded day | file | 1 | replaced | replaced: new version of this day replaced the previous one | PASS |
| row: invalid_price | row | 3 | 3 quarantined as invalid_price | 3 quarantined | PASS |
| row: invalid_quantity | row | 3 | 3 quarantined as invalid_quantity | 3 quarantined | PASS |
| row: invalid_timestamp | row | 3 | 3 quarantined as invalid_timestamp | 3 quarantined | PASS |
| row: missing_invoice | row | 2 | 2 quarantined as missing_invoice | 2 quarantined | PASS |
| row: missing_stock_code | row | 2 | 2 quarantined as missing_stock_code | 2 quarantined | PASS |
| row: future_date | row | 2 | 2 quarantined as future_date | 2 quarantined | PASS |
| row: negative_price | row | 2 | 2 quarantined as negative_price | 2 quarantined | PASS |
| row: test_product | row | 2 | 2 quarantined as test_product | 2 quarantined | PASS |
| row: cancellation_with_positive_quantity | row | 2 | 2 quarantined as cancellation_with_positive_quantity | 2 quarantined | PASS |
| row: exact duplicate lines | row | 5 | 5 more duplicates dropped | 5 dropped | PASS |
| row: unknown stock code | row | 1 | loaded as 'other' + warning | 1 flagged | PASS |
| row: late-arriving sale (dated day before) | row | 2 | loaded, flagged late | 2 flagged late | PASS |
| gold: late sale corrects the earlier day | row | 2 | 2010-11-30 revenue +£25.50 | +£25.50 | PASS |
| gold: faulty rows do not change revenue | row | 21 | 2010-12-01 revenue unchanged | change £0.00 | PASS |
| batch: customer IDs missing in whole file | batch | 1 | loaded + warning missing_customer_spike | warned | PASS |
| batch: 95% of the day's rows missing | batch | 1 | loaded + warning volume_drop | warned | PASS |
| batch: day's volume x6 | batch | 1 | loaded + warning volume_spike | warned | PASS |
| batch: 25% of rows sent twice | batch | 1 | loaded + warning duplicate_spike | warned | PASS |
| batch: duplicates in the 25% re-sent rows dropped | batch | 362 | 418 dropped | 418 dropped | PASS |
| exact re-send of 2010-12-01.csv | file | 1 | skipped_duplicate | skipped_duplicate | PASS |
| exact re-send of 2010-12-07.csv | file | 1 | skipped_duplicate | skipped_duplicate | PASS |
| re-sends leave silver unchanged | reliability | 2 | 6,013 rows | 6,013 rows | PASS |
| backfill of all replayed days | reliability | 6 | 9 tables identical | 9 identical | PASS |
| full rebuild from bronze | reliability | 1 | 9 tables identical | 9 identical | PASS |

## Batch anomaly false alarms on clean history

6 warnings on 299 clean history batches (2.0%):

| batch_date | anomaly |
|---|---|
| 2009-12-22 | missing_customer_spike |
| 2010-01-06 | volume_drop |
| 2010-01-11 | missing_customer_spike |
| 2010-01-13 | missing_customer_spike |
| 2010-04-19 | missing_customer_spike |
| 2010-06-04 | missing_customer_spike |

## Timings

| round2_ingest_and_build_s | backfill_s | full_rebuild_s |
|---|---|---|
| 7.6 | 7.0 | 13.6 |
