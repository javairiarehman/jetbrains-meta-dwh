-- =====================================================================
-- 05_fct_query_table.sql
-- One row per (query, table read). Runs after 04_fct_query.sql and
-- reuses user, team and resource values from fct_query.
--
-- Queries reading several tables: the query's CPU and memory are split
-- between its tables in proportion to the bytes read from each table.
-- If the connector did not report bytes for every table of the query,
-- the split is equal. The shares of one query always sum to 1, so
-- summing allocated resources over tables never double counts.
-- =====================================================================

DELETE FROM iceberg.meta_dwh.fct_query_table
WHERE query_date BETWEEN {{start_date}} AND {{end_date}};

INSERT INTO iceberg.meta_dwh.fct_query_table
WITH events AS (
    SELECT
        e.query_id,
        e.event_date,
        e.inputs,
        ROW_NUMBER() OVER (PARTITION BY e.query_id ORDER BY e.ingested_at DESC) AS rn
    FROM iceberg.raw.trino_query_events e
    WHERE e.event_date BETWEEN {{start_date}} AND {{end_date}}
),
inputs AS (
    SELECT
        e.query_id,
        e.event_date AS query_date,
        lower(i.catalog_name) AS catalog_name,
        lower(i.schema_name)  AS schema_name,
        lower(i.table_name)   AS table_name,
        i.columns,
        i.physical_input_bytes
    FROM events e
    CROSS JOIN UNNEST(e.inputs) AS i (catalog_name, schema_name, table_name,
                                      columns, physical_input_bytes, physical_input_rows)
    WHERE e.rn = 1
      AND lower(i.catalog_name) <> 'system'
      AND lower(i.schema_name)  <> 'information_schema'
),
per_table AS (
    -- the same table can appear more than once in one query (self-join)
    SELECT
        query_id,
        query_date,
        catalog_name,
        schema_name,
        table_name,
        array_distinct(flatten(array_agg(columns) FILTER (WHERE columns IS NOT NULL))) AS accessed_columns,
        SUM(physical_input_bytes)   AS table_scanned_bytes,
        COUNT(physical_input_bytes) AS reported_inputs,
        COUNT(*)                    AS all_inputs
    FROM inputs
    GROUP BY query_id, query_date, catalog_name, schema_name, table_name
),
shares AS (
    SELECT
        p.*,
        COUNT(*) OVER (PARTITION BY query_id)                AS tables_in_query,
        SUM(CASE WHEN reported_inputs = all_inputs THEN 1 ELSE 0 END)
            OVER (PARTITION BY query_id)                     AS tables_with_bytes,
        SUM(table_scanned_bytes) OVER (PARTITION BY query_id) AS query_table_bytes
    FROM per_table p
),
allocated AS (
    SELECT
        s.*,
        CASE
            WHEN tables_with_bytes = tables_in_query AND query_table_bytes > 0
                THEN CAST(table_scanned_bytes AS DOUBLE) / query_table_bytes
            ELSE 1e0 / tables_in_query
        END AS resource_share
    FROM shares s
)
SELECT
    a.query_id,
    a.query_date,
    concat_ws('.', a.catalog_name, a.schema_name, a.table_name) AS table_key,
    a.catalog_name,
    a.schema_name,
    a.table_name,
    q.user_name,
    q.team_id,
    q.user_type,
    q.query_state,
    a.accessed_columns,
    CAST(COALESCE(cardinality(a.accessed_columns), 0) AS INTEGER) AS accessed_column_count,
    a.table_scanned_bytes,
    a.resource_share,
    q.cpu_seconds       * a.resource_share                       AS allocated_cpu_seconds,
    q.memory_gb_seconds * a.resource_share                       AS allocated_memory_gb_seconds,
    COALESCE(CAST(a.table_scanned_bytes AS DOUBLE),
             q.scanned_bytes * a.resource_share)                 AS allocated_scanned_bytes,
    current_timestamp                                            AS loaded_at
FROM allocated a
JOIN iceberg.meta_dwh.fct_query q
  ON q.query_id   = a.query_id
 AND q.query_date = a.query_date
WHERE q.query_date BETWEEN {{start_date}} AND {{end_date}};
