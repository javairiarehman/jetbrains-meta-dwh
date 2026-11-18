-- =====================================================================
-- 04_fct_query.sql
-- Incremental load of query-level facts for the window
-- [{{start_date}}, {{end_date}}]. The window is deleted and reloaded,
-- so the script is idempotent and can be used for backfills.
-- =====================================================================

DELETE FROM iceberg.meta_dwh.fct_query
WHERE query_date BETWEEN {{start_date}} AND {{end_date}};

INSERT INTO iceberg.meta_dwh.fct_query
WITH events AS (
    -- the event listener delivers at least once: keep the latest copy
    SELECT
        e.*,
        ROW_NUMBER() OVER (PARTITION BY e.query_id ORDER BY e.ingested_at DESC) AS rn
    FROM iceberg.raw.trino_query_events e
    WHERE e.event_date BETWEEN {{start_date}} AND {{end_date}}
)
SELECT
    e.query_id,
    e.event_date                                          AS query_date,
    e.create_time                                         AS submitted_at,
    e.execution_start_time                                AS started_at,
    e.end_time                                            AS ended_at,
    e."user"                                              AS user_name,
    COALESCE(ut.team_id, 'unmapped')                      AS team_id,
    COALESCE(ut.user_type, 'unknown')                     AS user_type,
    e.source,
    e.query_type,
    e.query_state,
    e.error_code,
    e.resource_group_id,
    CAST(COALESCE(cardinality(e.inputs), 0) AS INTEGER)   AS input_table_count,
    CASE WHEN e.output IS NOT NULL
         THEN lower(concat_ws('.', e.output.catalog_name, e.output.schema_name, e.output.table_name))
    END                                                   AS output_table_key,
    CAST(e.cpu_time_ms    AS DOUBLE) / 1000               AS cpu_seconds,
    CAST(e.wall_time_ms   AS DOUBLE) / 1000               AS wall_seconds,
    CAST(e.queued_time_ms AS DOUBLE) / 1000               AS queued_seconds,
    e.peak_user_memory_bytes                              AS peak_memory_bytes,
    e.cumulative_memory / 1000 / 1e9                      AS memory_gb_seconds,  -- byte-ms -> GB-s
    e.physical_input_bytes                                AS scanned_bytes,
    e.physical_input_rows                                 AS scanned_rows,
    e.output_bytes,
    e.written_bytes,
    current_timestamp                                     AS loaded_at
FROM events e
LEFT JOIN iceberg.meta_dwh.dim_user_team ut
       ON ut.user_name = e."user"
      AND e.event_date >= ut.valid_from
      AND e.event_date <  ut.valid_to
WHERE e.rn = 1;
