-- =====================================================================
-- 07_agg_table_user_daily.sql
-- Daily usage per (table, user). Keeping the user in the grain lets us
-- count distinct users correctly over any period (daily distinct counts
-- cannot be summed).
-- =====================================================================

DELETE FROM iceberg.meta_dwh.agg_table_user_daily
WHERE usage_date BETWEEN {{start_date}} AND {{end_date}};

INSERT INTO iceberg.meta_dwh.agg_table_user_daily
SELECT
    query_date                                AS usage_date,
    table_key,
    user_name,
    team_id,
    user_type,
    COUNT(*)                                  AS query_count,
    COUNT_IF(query_state = 'FAILED')          AS failed_query_count,
    SUM(allocated_scanned_bytes)              AS scanned_bytes,
    SUM(allocated_cpu_seconds)                AS allocated_cpu_seconds,
    SUM(allocated_memory_gb_seconds)          AS allocated_memory_gb_seconds,
    current_timestamp                         AS loaded_at
FROM iceberg.meta_dwh.fct_query_table
WHERE query_date BETWEEN {{start_date}} AND {{end_date}}
GROUP BY query_date, table_key, user_name, team_id, user_type;
