-- Warehouse objects the pipeline owns. Every statement is safe to run again.
-- dbt owns the staging, intermediate and marts schemas.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS quarantine;
CREATE SCHEMA IF NOT EXISTS ops;

-- Rows that passed every rule. One list partition per month, replaced as a unit by the loader.
CREATE TABLE IF NOT EXISTS raw.yellow_trips (
    source_month           date          NOT NULL,
    source_row             bigint        NOT NULL,   -- row number inside the month's file
    vendorid               smallint      NOT NULL,
    tpep_pickup_datetime   timestamp     NOT NULL,
    tpep_dropoff_datetime  timestamp     NOT NULL,
    passenger_count        smallint,
    trip_distance          numeric(12,2) NOT NULL,
    ratecodeid             integer,
    store_and_fwd_flag     text,
    pulocationid           smallint      NOT NULL,
    dolocationid           smallint      NOT NULL,
    payment_type           smallint      NOT NULL,
    fare_amount            numeric(12,2) NOT NULL,
    extra                  numeric(12,2),
    mta_tax                numeric(12,2),
    tip_amount             numeric(12,2),
    tolls_amount           numeric(12,2),
    improvement_surcharge  numeric(12,2),
    total_amount           numeric(12,2) NOT NULL,
    congestion_surcharge   numeric(12,2),
    airport_fee            numeric(12,2),
    cbd_congestion_fee     numeric(12,2),
    PRIMARY KEY (source_month, source_row)
) PARTITION BY LIST (source_month);

-- Rows that failed at least one rule, exactly as received, with every reason code that applied.
CREATE TABLE IF NOT EXISTS quarantine.yellow_trips (
    source_month           date             NOT NULL,
    source_row             bigint           NOT NULL,
    reason_codes           text[]           NOT NULL,
    primary_reason         text             NOT NULL,
    vendorid               double precision,
    tpep_pickup_datetime   timestamp,
    tpep_dropoff_datetime  timestamp,
    passenger_count        double precision,
    trip_distance          double precision,
    ratecodeid             double precision,
    store_and_fwd_flag     text,
    pulocationid           double precision,
    dolocationid           double precision,
    payment_type           double precision,
    fare_amount            double precision,
    extra                  double precision,
    mta_tax                double precision,
    tip_amount             double precision,
    tolls_amount           double precision,
    improvement_surcharge  double precision,
    total_amount           double precision,
    congestion_surcharge   double precision,
    airport_fee            double precision,
    cbd_congestion_fee     double precision,
    PRIMARY KEY (source_month, source_row)
) PARTITION BY LIST (source_month);

CREATE TABLE IF NOT EXISTS raw.taxi_zone_lookup (
    locationid    smallint PRIMARY KEY,
    borough       text,
    zone          text,
    service_zone  text
);

-- One row per month: what the last successful load saw. Written in the same transaction as the data.
CREATE TABLE IF NOT EXISTS ops.ingestion_ledger (
    source_month      date PRIMARY KEY,
    landing_key       text        NOT NULL,
    file_bytes        bigint      NOT NULL,
    file_sha256       text        NOT NULL,
    fetched_at        timestamptz NOT NULL,
    rows_in           bigint      NOT NULL,
    rows_loaded       bigint      NOT NULL,
    rows_quarantined  bigint      NOT NULL,
    warn_counts       jsonb       NOT NULL,
    validated_at      timestamptz NOT NULL,
    loaded_at         timestamptz NOT NULL,
    load_count        integer     NOT NULL DEFAULT 1,
    run_id            text        NOT NULL,
    CONSTRAINT ledger_rows_add_up CHECK (rows_in = rows_loaded + rows_quarantined)
);

-- Rows per reason code for the month's last load.
CREATE TABLE IF NOT EXISTS ops.ingestion_checks (
    source_month  date   NOT NULL,
    reason_code   text   NOT NULL,
    rows_failed   bigint NOT NULL,   -- rows that failed this rule (a row can fail several)
    rows_primary  bigint NOT NULL,   -- rows whose first failing rule was this one
    PRIMARY KEY (source_month, reason_code)
);

-- Append-only: one row per stage attempt, used for run-time measurements.
CREATE TABLE IF NOT EXISTS ops.stage_timings (
    id            bigserial PRIMARY KEY,
    run_id        text        NOT NULL,
    source_month  date,
    stage         text        NOT NULL,
    started_at    timestamptz NOT NULL,
    finished_at   timestamptz NOT NULL,
    seconds       numeric(12,3) NOT NULL,
    rows          bigint,
    detail        jsonb
);
CREATE INDEX IF NOT EXISTS stage_timings_month ON ops.stage_timings (source_month, stage);
