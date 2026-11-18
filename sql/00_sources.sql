-- =====================================================================
-- 00_sources.sql
-- Assumed source tables. They are not created by this pipeline; they
-- are documented here so the transformations are easy to follow.
-- Engine: Trino, catalog "iceberg", tables stored on S3.
-- =====================================================================

-- 1) Raw Trino query events.
-- One row per QueryCompletedEvent emitted by a Trino event listener
-- (fields mirror io.trino.spi.eventlistener.QueryCompletedEvent).
-- Appended continuously; may contain duplicates (at-least-once delivery)
-- and late-arriving rows.
CREATE TABLE IF NOT EXISTS iceberg.raw.trino_query_events (
    query_id                VARCHAR,          -- e.g. 20261118_101500_00042_abcde
    query_state             VARCHAR,          -- FINISHED | FAILED
    error_code              VARCHAR,          -- NULL if succeeded
    query_type              VARCHAR,          -- SELECT | INSERT | MERGE | DESCRIBE | ...
    "user"                  VARCHAR,          -- session user (human or service account)
    principal               VARCHAR,
    source                  VARCHAR,          -- client: datagrip, airflow, superset, ...
    resource_group_id       VARCHAR,
    create_time             TIMESTAMP(6) WITH TIME ZONE,
    execution_start_time    TIMESTAMP(6) WITH TIME ZONE,
    end_time                TIMESTAMP(6) WITH TIME ZONE,
    -- resource statistics (QueryStatistics)
    cpu_time_ms             BIGINT,
    wall_time_ms            BIGINT,
    queued_time_ms          BIGINT,
    peak_user_memory_bytes  BIGINT,
    cumulative_memory       DOUBLE,           -- byte-milliseconds (memory x time)
    physical_input_bytes    BIGINT,           -- bytes read from S3
    physical_input_rows     BIGINT,
    output_bytes            BIGINT,
    output_rows             BIGINT,
    written_bytes           BIGINT,
    -- tables read by the query (QueryIOMetadata.inputs).
    -- Views are already resolved to the underlying base tables by Trino.
    inputs ARRAY(ROW(
        catalog_name          VARCHAR,
        schema_name           VARCHAR,
        table_name            VARCHAR,
        columns               ARRAY(VARCHAR),
        physical_input_bytes  BIGINT,          -- may be NULL (connector dependent)
        physical_input_rows   BIGINT
    )),
    -- table written by the query (QueryIOMetadata.output), NULL for reads
    output ROW(catalog_name VARCHAR, schema_name VARCHAR, table_name VARCHAR),
    query_text              VARCHAR,
    ingested_at             TIMESTAMP(6) WITH TIME ZONE,  -- when the row landed in raw
    event_date              DATE                           -- partition = DATE(end_time) in UTC
)
WITH (format = 'PARQUET', partitioning = ARRAY['event_date']);

-- 2) Daily snapshot of user -> team assignments, exported from the
-- identity provider / HR system (LDAP groups, Okta, ...).
-- Service accounts are included and assigned to their owning team.
CREATE TABLE IF NOT EXISTS iceberg.ref.user_team_snapshot (
    snapshot_date  DATE,
    user_name      VARCHAR,      -- must match Trino session user
    team_id        VARCHAR,
    user_type      VARCHAR       -- human | service
)
WITH (format = 'PARQUET', partitioning = ARRAY['snapshot_date']);

-- 3) Team reference data.
CREATE TABLE IF NOT EXISTS iceberg.ref.teams (
    team_id     VARCHAR,
    team_name   VARCHAR,
    department  VARCHAR
);

-- 4) Optional: table ownership, maintained by data owners (for example
-- generated from a YAML file in the data platform repository).
-- Used to report usage of a team's tables by other teams and to find
-- unused tables per owner.
CREATE TABLE IF NOT EXISTS iceberg.ref.table_owners (
    catalog_name    VARCHAR,
    schema_name     VARCHAR,
    table_name      VARCHAR,
    owner_team_id   VARCHAR,
    description     VARCHAR
);
