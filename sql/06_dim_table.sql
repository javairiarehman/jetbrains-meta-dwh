-- =====================================================================
-- 06_dim_table.sql
-- Upsert of tables seen in the load window. first_seen_date only moves
-- back (backfills), last_seen_date only moves forward.
-- Owner comes from the optional ref.table_owners table.
-- =====================================================================

MERGE INTO iceberg.meta_dwh.dim_table t
USING (
    SELECT
        f.table_key,
        f.catalog_name,
        f.schema_name,
        f.table_name,
        o.owner_team_id,
        MIN(f.query_date) AS first_seen_date,
        MAX(f.query_date) AS last_seen_date
    FROM iceberg.meta_dwh.fct_query_table f
    LEFT JOIN iceberg.ref.table_owners o
           ON lower(o.catalog_name) = f.catalog_name
          AND lower(o.schema_name)  = f.schema_name
          AND lower(o.table_name)   = f.table_name
    WHERE f.query_date BETWEEN {{start_date}} AND {{end_date}}
    GROUP BY f.table_key, f.catalog_name, f.schema_name, f.table_name, o.owner_team_id
) s
ON t.table_key = s.table_key
WHEN MATCHED THEN UPDATE SET
    owner_team_id   = COALESCE(s.owner_team_id, t.owner_team_id),
    first_seen_date = least(t.first_seen_date, s.first_seen_date),
    last_seen_date  = greatest(t.last_seen_date, s.last_seen_date),
    updated_at      = current_timestamp
WHEN NOT MATCHED THEN INSERT (
    table_key, catalog_name, schema_name, table_name, owner_team_id,
    first_seen_date, last_seen_date, updated_at
) VALUES (
    s.table_key, s.catalog_name, s.schema_name, s.table_name, s.owner_team_id,
    s.first_seen_date, s.last_seen_date, current_timestamp
);
