-- =====================================================================
-- 03_dim_user_team.sql
-- User -> team history (SCD type 2) built from daily snapshots.
-- Consecutive snapshots with the same team are collapsed into one
-- membership period [valid_from, valid_to).
--
-- Rules:
--   * the first known period of a user starts at 1970-01-01, so that
--     queries from before snapshots were collected (backfills) are
--     attributed to the first known team;
--   * the latest period is open-ended (valid_to = 9999-12-31).
-- Full rebuild: the snapshot table is small (users x days).
-- =====================================================================

CREATE OR REPLACE TABLE iceberg.meta_dwh.dim_user_team
WITH (format = 'PARQUET')
AS
WITH snapshots AS (
    SELECT
        snapshot_date,
        user_name,
        team_id,
        user_type,
        LAG(team_id) OVER (PARTITION BY user_name ORDER BY snapshot_date) AS prev_team_id
    FROM iceberg.ref.user_team_snapshot
),
grouped AS (
    SELECT
        snapshot_date,
        user_name,
        team_id,
        user_type,
        SUM(CASE WHEN prev_team_id IS NULL OR prev_team_id <> team_id THEN 1 ELSE 0 END)
            OVER (PARTITION BY user_name ORDER BY snapshot_date
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS period_no
    FROM snapshots
),
periods AS (
    SELECT
        user_name,
        team_id,
        max_by(user_type, snapshot_date) AS user_type,
        MIN(snapshot_date)               AS period_start
    FROM grouped
    GROUP BY user_name, team_id, period_no
),
ordered AS (
    SELECT
        user_name,
        team_id,
        user_type,
        period_start,
        ROW_NUMBER() OVER (PARTITION BY user_name ORDER BY period_start) AS rn,
        LEAD(period_start) OVER (PARTITION BY user_name ORDER BY period_start) AS next_start
    FROM periods
)
SELECT
    user_name,
    team_id,
    user_type,
    CASE WHEN rn = 1 THEN DATE '1970-01-01' ELSE period_start END AS valid_from,
    COALESCE(next_start, DATE '9999-12-31')                     AS valid_to,
    next_start IS NULL                                           AS is_current,
    current_timestamp                                            AS updated_at
FROM ordered;
