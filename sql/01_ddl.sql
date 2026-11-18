-- =====================================================================
-- 01_ddl.sql
-- Meta DWH tables. Facts and aggregates are partitioned by day so that
-- a daily reload only rewrites a few partitions.
-- Dimensions (dim_team, dim_user_team) are small and fully rebuilt by
-- their load scripts with CREATE OR REPLACE TABLE.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.meta_dwh;

-- One row per table that has ever been read.
CREATE TABLE IF NOT EXISTS iceberg.meta_dwh.dim_table (
    table_key        VARCHAR,     -- catalog.schema.table
    catalog_name     VARCHAR,
    schema_name      VARCHAR,
    table_name       VARCHAR,
    owner_team_id    VARCHAR,     -- from ref.table_owners, NULL if unknown
    first_seen_date  DATE,
    last_seen_date   DATE,
    updated_at       TIMESTAMP(6) WITH TIME ZONE
)
WITH (format = 'PARQUET');

-- One row per executed query.
CREATE TABLE IF NOT EXISTS iceberg.meta_dwh.fct_query (
    query_id             VARCHAR,
    query_date           DATE,        -- UTC date of end_time
    submitted_at         TIMESTAMP(6) WITH TIME ZONE,
    started_at           TIMESTAMP(6) WITH TIME ZONE,
    ended_at             TIMESTAMP(6) WITH TIME ZONE,
    user_name            VARCHAR,
    team_id              VARCHAR,     -- team of the user at query time
    user_type            VARCHAR,     -- human | service | unknown
    source               VARCHAR,
    query_type           VARCHAR,
    query_state          VARCHAR,     -- FINISHED | FAILED
    error_code           VARCHAR,
    resource_group_id    VARCHAR,
    input_table_count    INTEGER,
    output_table_key     VARCHAR,
    cpu_seconds          DOUBLE,
    wall_seconds         DOUBLE,
    queued_seconds       DOUBLE,
    peak_memory_bytes    BIGINT,
    memory_gb_seconds    DOUBLE,
    scanned_bytes        BIGINT,
    scanned_rows         BIGINT,
    output_bytes         BIGINT,
    written_bytes        BIGINT,
    loaded_at            TIMESTAMP(6) WITH TIME ZONE
)
WITH (format = 'PARQUET', partitioning = ARRAY['query_date']);

-- One row per (query, table read by the query).
-- Query resources are split between the tables the query read.
CREATE TABLE IF NOT EXISTS iceberg.meta_dwh.fct_query_table (
    query_id                     VARCHAR,
    query_date                   DATE,
    table_key                    VARCHAR,
    catalog_name                 VARCHAR,
    schema_name                  VARCHAR,
    table_name                   VARCHAR,
    user_name                    VARCHAR,
    team_id                      VARCHAR,
    user_type                    VARCHAR,
    query_state                  VARCHAR,
    accessed_columns             ARRAY(VARCHAR),
    accessed_column_count        INTEGER,
    table_scanned_bytes          BIGINT,   -- NULL if not reported by the connector
    resource_share               DOUBLE,   -- 0..1, sums to 1 per query
    allocated_cpu_seconds        DOUBLE,
    allocated_memory_gb_seconds  DOUBLE,
    allocated_scanned_bytes      DOUBLE,
    loaded_at                    TIMESTAMP(6) WITH TIME ZONE
)
WITH (format = 'PARQUET', partitioning = ARRAY['query_date']);

-- One row per (day, table, user). Answers distinct-user questions for
-- any period without scanning the query-level facts.
CREATE TABLE IF NOT EXISTS iceberg.meta_dwh.agg_table_user_daily (
    usage_date                   DATE,
    table_key                    VARCHAR,
    user_name                    VARCHAR,
    team_id                      VARCHAR,
    user_type                    VARCHAR,
    query_count                  BIGINT,
    failed_query_count           BIGINT,
    scanned_bytes                DOUBLE,
    allocated_cpu_seconds        DOUBLE,
    allocated_memory_gb_seconds  DOUBLE,
    loaded_at                    TIMESTAMP(6) WITH TIME ZONE
)
WITH (format = 'PARQUET', partitioning = ARRAY['usage_date']);

-- One row per (day, team). Answers resource questions per team.
CREATE TABLE IF NOT EXISTS iceberg.meta_dwh.agg_team_daily (
    usage_date           DATE,
    team_id              VARCHAR,
    query_count          BIGINT,
    failed_query_count   BIGINT,
    distinct_users       BIGINT,      -- per day only, not additive across days
    cpu_seconds          DOUBLE,
    memory_gb_seconds    DOUBLE,
    wall_seconds         DOUBLE,
    queued_seconds       DOUBLE,
    scanned_bytes        BIGINT,
    written_bytes        BIGINT,
    loaded_at            TIMESTAMP(6) WITH TIME ZONE
)
WITH (format = 'PARQUET', partitioning = ARRAY['usage_date']);
