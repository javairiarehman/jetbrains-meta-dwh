-- =====================================================================
-- 08_agg_team_daily.sql
-- Daily resource consumption per team. Built from fct_query (one row
-- per query) so that queries reading several tables are counted once.
-- =====================================================================

DELETE FROM iceberg.meta_dwh.agg_team_daily
WHERE usage_date BETWEEN {{start_date}} AND {{end_date}};

INSERT INTO iceberg.meta_dwh.agg_team_daily
SELECT
    query_date                          AS usage_date,
    team_id,
    COUNT(*)                            AS query_count,
    COUNT_IF(query_state = 'FAILED')    AS failed_query_count,
    COUNT(DISTINCT user_name)           AS distinct_users,
    SUM(cpu_seconds)                    AS cpu_seconds,
    SUM(memory_gb_seconds)              AS memory_gb_seconds,
    SUM(wall_seconds)                   AS wall_seconds,
    SUM(queued_seconds)                 AS queued_seconds,
    SUM(scanned_bytes)                  AS scanned_bytes,
    SUM(written_bytes)                  AS written_bytes,
    current_timestamp                   AS loaded_at
FROM iceberg.meta_dwh.fct_query
WHERE query_date BETWEEN {{start_date}} AND {{end_date}}
GROUP BY query_date, team_id;
