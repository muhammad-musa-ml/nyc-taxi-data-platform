{{ config(materialized='table') }}

-- For the data owner: how many rows each month brought in, how many were loaded and how many were held
-- back, split by the rule that rejected them. A month with no rejected rows has one row with a null reason.
select
    l.source_month,
    l.rows_in,
    l.rows_loaded,
    l.rows_quarantined,
    round(100.0 * l.rows_quarantined / nullif(l.rows_in, 0), 4)   as quarantine_pct,
    c.reason_code,
    c.rows_failed,
    c.rows_primary,
    l.file_sha256,
    l.loaded_at
from {{ source('ops', 'ingestion_ledger') }} l
left join {{ source('ops', 'ingestion_checks') }} c using (source_month)
