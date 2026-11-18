-- =====================================================================
-- 09_example_queries.sql
-- Example analytical queries on the Meta DWH.
-- Table X = iceberg.analytics.product_events, team Y = 'product-analytics'
-- =====================================================================


-- Q1. How many distinct users queried table X during a given period?
SELECT
    COUNT(DISTINCT user_name) AS distinct_users,
    SUM(query_count)          AS queries
FROM iceberg.meta_dwh.agg_table_user_daily
WHERE table_key = 'iceberg.analytics.product_events'
  AND usage_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-31';


-- Q1b. Same question as a weekly trend, humans and service accounts separated.
SELECT
    date_trunc('week', usage_date)                                   AS week,
    COUNT(DISTINCT CASE WHEN user_type = 'human'   THEN user_name END) AS human_users,
    COUNT(DISTINCT CASE WHEN user_type = 'service' THEN user_name END) AS service_accounts,
    SUM(query_count)                                                   AS queries
FROM iceberg.meta_dwh.agg_table_user_daily
WHERE table_key = 'iceberg.analytics.product_events'
  AND usage_date >= DATE '2026-06-01'
GROUP BY 1
ORDER BY 1;


-- Q2. How many resources did team Y consume during a given period?
SELECT
    t.team_name,
    SUM(a.query_count)                                 AS queries,
    SUM(a.failed_query_count)                          AS failed_queries,
    round(SUM(a.cpu_seconds) / 3600, 2)                AS cpu_hours,
    round(SUM(a.memory_gb_seconds) / 3600, 2)          AS memory_gb_hours,
    round(SUM(a.scanned_bytes) / 1e12, 3)              AS scanned_tb,
    round(SUM(a.written_bytes) / 1e9, 2)               AS written_gb
FROM iceberg.meta_dwh.agg_team_daily a
JOIN iceberg.meta_dwh.dim_team t ON t.team_id = a.team_id
WHERE a.team_id = 'product-analytics'
  AND a.usage_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-31'
GROUP BY t.team_name;


-- Q2b. Monthly trend of team Y's consumption.
SELECT
    date_trunc('month', usage_date)               AS month,
    SUM(query_count)                              AS queries,
    round(SUM(cpu_seconds) / 3600, 2)             AS cpu_hours,
    round(SUM(memory_gb_seconds) / 3600, 2)       AS memory_gb_hours,
    round(SUM(scanned_bytes) / 1e12, 3)           AS scanned_tb
FROM iceberg.meta_dwh.agg_team_daily
WHERE team_id = 'product-analytics'
GROUP BY 1
ORDER BY 1;


-- Q2c. Which tables did team Y spend its resources on?
SELECT
    table_key,
    SUM(query_count)                                  AS queries,
    round(SUM(allocated_cpu_seconds) / 3600, 2)       AS cpu_hours,
    round(SUM(scanned_bytes) / 1e12, 3)               AS scanned_tb
FROM iceberg.meta_dwh.agg_table_user_daily
WHERE team_id = 'product-analytics'
  AND usage_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-31'
GROUP BY table_key
ORDER BY cpu_hours DESC
LIMIT 20;


-- Q3. Tables not queried in the last 90 days (candidates for cleanup).
SELECT
    d.table_key,
    d.owner_team_id,
    d.last_seen_date
FROM iceberg.meta_dwh.dim_table d
WHERE d.last_seen_date < current_date - INTERVAL '90' DAY
ORDER BY d.last_seen_date;


-- Q4. Who uses team Y's tables? Usage of owned tables by other teams.
SELECT
    u.team_id                       AS consumer_team,
    COUNT(DISTINCT u.user_name)     AS distinct_users,
    SUM(u.query_count)              AS queries
FROM iceberg.meta_dwh.agg_table_user_daily u
JOIN iceberg.meta_dwh.dim_table d ON d.table_key = u.table_key
WHERE d.owner_team_id = 'product-analytics'
  AND u.team_id <> 'product-analytics'
  AND u.usage_date >= current_date - INTERVAL '30' DAY
GROUP BY u.team_id
ORDER BY queries DESC;
