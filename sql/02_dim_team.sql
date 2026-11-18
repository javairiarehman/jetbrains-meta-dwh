-- =====================================================================
-- 02_dim_team.sql
-- Full rebuild (small table). Adds a technical "unmapped" team so that
-- queries of users without a known team are still counted.
-- =====================================================================

CREATE OR REPLACE TABLE iceberg.meta_dwh.dim_team
WITH (format = 'PARQUET')
AS
SELECT
    team_id,
    team_name,
    department,
    current_timestamp AS updated_at
FROM iceberg.ref.teams
UNION ALL
SELECT
    'unmapped',
    'Unmapped users',
    'n/a',
    current_timestamp;
