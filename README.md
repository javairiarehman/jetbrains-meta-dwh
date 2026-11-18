# Meta DWH for Trino table usage


Author: Javairia Rehman
A small data warehouse that collects usage statistics of the Iceberg tables
queried through Trino. It answers questions such as

* how many distinct users queried table X during a given period,
* how many resources team Y consumed during that period,

and keeps daily history so that changes in usage can be followed over time.

Everything runs in Trino on the Iceberg catalog. SQL does the transformations,
a short Python script orchestrates daily loads and backfills.

```
sql/
  00_sources.sql              assumed source tables (documentation only)
  01_ddl.sql                  Meta DWH tables
  02_dim_team.sql             team dimension (full rebuild)
  03_dim_user_team.sql        user -> team history, SCD2 (full rebuild)
  04_fct_query.sql            query facts (incremental)
  05_fct_query_table.sql      query x table facts with resource split (incremental)
  06_dim_table.sql            table dimension (merge)
  07_agg_table_user_daily.sql daily table usage per user (incremental)
  08_agg_team_daily.sql       daily resources per team (incremental)
  09_example_queries.sql      queries answering the questions above
pipeline/run_pipeline.py      runs the scripts for a date range
```

## Sources and assumptions

**Raw query events** (`iceberg.raw.trino_query_events`). Trino emits a
`QueryCompletedEvent` for every query through an event listener. I assume these
events are already written to S3 as an Iceberg table, one row per event,
partitioned by `event_date` (UTC date of the query end time). The fields follow
the Trino SPI: query id, user, source, query type, state, timestamps, resource
statistics and the list of input tables (`inputs`) with the columns read and
the physical bytes read per table. Views are already resolved to their base
tables by Trino, so usage of a view is counted on the underlying tables.

Further assumptions about the raw data:

* delivery is at least once, so a query can appear more than once; the latest
  copy (by `ingested_at`) is kept;
* events for a day can arrive up to a few days late;
* `cumulative_memory` is in byte-milliseconds;
* per-table `physical_input_bytes` can be missing for some connectors.

**User to team mapping** (`iceberg.ref.user_team_snapshot`). A daily snapshot
exported from the identity provider (LDAP/Okta group membership):
`snapshot_date, user_name, team_id, user_type`. A user belongs to one primary
team at a time. Service accounts (Airflow, BI tools) are part of the same export
and are assigned to the team that owns them, with `user_type = 'service'`.
The Trino session user is the same name as in the identity provider.

**Teams** (`iceberg.ref.teams`): `team_id, team_name, department`.

**Table owners** (`iceberg.ref.table_owners`, optional additional metadata):
`catalog_name, schema_name, table_name, owner_team_id, description`, maintained
by data owners, for example from a YAML file in the data platform repository.
It lets us report who uses a team's tables and find unused tables per owner.
Without it the model still works, `owner_team_id` is just NULL.

## Data model

```
dim_team 1 ──< dim_user_team
    │
    1
    │
    ^
fct_query 1 ──< fct_query_table >── 1 dim_table
    │                  │
    ▼                  ▼
agg_team_daily   agg_table_user_daily
```

| Table | Grain | Load |
|---|---|---|
| `dim_team` | one row per team, plus a technical `unmapped` team | full rebuild |
| `dim_user_team` | one row per user and membership period `[valid_from, valid_to)` | full rebuild |
| `dim_table` | one row per table that has been read (`catalog.schema.table`) | merge |
| `fct_query` | one row per query | delete and insert per day |
| `fct_query_table` | one row per query and table read by it | delete and insert per day |
| `agg_table_user_daily` | one row per day, table and user | delete and insert per day |
| `agg_team_daily` | one row per day and team | delete and insert per day |

Facts and aggregates are partitioned by day, so a daily load rewrites only a few
partitions and queries for a period read only the matching partitions.

### Why two fact tables

`fct_query` holds the resources of a query once. `fct_query_table` repeats a
query for every table it read. Team resource numbers are therefore always taken
from `fct_query` (or `agg_team_daily`), otherwise a query reading three tables
would be counted three times. Table usage is taken from `fct_query_table`.

### Queries accessing several tables

Each row in `fct_query_table` gets a `resource_share` between 0 and 1:

* if bytes read are known for every table of the query, the share is the
  table's bytes divided by the query's total bytes,
* otherwise the query's resources are split equally between its tables.

The shares of one query sum to 1, so allocated CPU and memory per table add up
to the query total. This makes it possible to say which tables a team's
resources were spent on. A table that is read several times in one query
(self-join) is collapsed into one row. Queries without any input table (for
example `SHOW` statements) stay in `fct_query` and are still counted for the
team. `system` and `information_schema` are excluded from table usage.

### Distinct users

Distinct user counts cannot be summed across days: a user active on Monday and
Tuesday would be counted twice for the week. `agg_table_user_daily` therefore
keeps the user in its grain. Counting distinct users for any period is a
`COUNT(DISTINCT user_name)` over a small table instead of a scan of all query
events. `agg_team_daily.distinct_users` is a per-day value only.

If the aggregate grows too large, the user column can be replaced by a
HyperLogLog sketch per day and table (`approx_set` in Trino), which can be
merged over any period with a small error.

### User to team attribution

Queries are attributed to the team of the user **at the time of the query**,
using the SCD2 history in `dim_user_team`. When someone changes teams, their
earlier queries stay with the old team, so historical reports do not change.
The first known period of a user starts at 1970-01-01, so backfilled queries
from before the snapshots existed go to the first known team. Users without
any mapping are put into the `unmapped` team, so totals stay complete and the
mapping gaps are visible. For a "current team" view, join on
`dim_user_team.is_current` instead.

## Resource usage metrics

All taken from the Trino query statistics:

| Metric | Meaning |
|---|---|
| `cpu_seconds` | CPU time used by the query on all workers, main cost driver of the cluster |
| `memory_gb_seconds` | memory reserved over time (cumulative memory), shows memory-heavy queries |
| `peak_memory_bytes` | highest memory reservation, relevant for failures and cluster sizing |
| `scanned_bytes` | bytes read from S3, drives S3 request and transfer cost and query time |
| `written_bytes` | bytes written, for example by CTAS and INSERT |
| `wall_seconds`, `queued_seconds` | duration and waiting time, user-facing performance |
| `query_count`, `failed_query_count` | activity and failure rate |

Failed queries are kept with `query_state = 'FAILED'`, because they consumed
resources as well. CPU hours and memory GB-hours can be turned into a cost
estimate by multiplying them with a rate per unit if needed.

## How data is updated

The pipeline runs once a day (for example from Airflow or cron) after midnight
UTC:

1. `01`–`03`: create tables if missing and rebuild the small dimensions.
2. `04`–`08`: for the load window, delete the window's partitions and insert
   them again, in dependency order (`fct_query` → `fct_query_table` →
   `dim_table` → aggregates).

The default window is the **last 3 days**, so late-arriving events are picked
up. Because every step replaces whole days, a run can be repeated safely and
gives the same result.

**Backfills and recalculations** use the same code with a different date range,
split into chunks:

```bash
python pipeline/run_pipeline.py --start-date 2026-01-01 --end-date 2026-06-30 --chunk-days 7
```

When the logic changes (for example the resource split), the affected period is
recalculated the same way. When the team mapping is corrected, `dim_user_team`
is rebuilt and the affected period is reloaded.

`dim_table` is merged: `first_seen_date` can only move back (backfills) and
`last_seen_date` only forward.

Old raw events can be expired with Iceberg snapshot expiration and a retention
policy on the raw table. The Meta DWH tables are small and kept for the full
history.

## Example queries

See `sql/09_example_queries.sql`. The two main questions:

```sql
-- distinct users of table X in a period
SELECT COUNT(DISTINCT user_name) AS distinct_users,
       SUM(query_count)          AS queries
FROM iceberg.meta_dwh.agg_table_user_daily
WHERE table_key = 'iceberg.analytics.product_events'
  AND usage_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-31';

-- resources consumed by team Y in a period
SELECT SUM(query_count)                          AS queries,
       round(SUM(cpu_seconds) / 3600, 2)         AS cpu_hours,
       round(SUM(memory_gb_seconds) / 3600, 2)   AS memory_gb_hours,
       round(SUM(scanned_bytes) / 1e12, 3)       AS scanned_tb
FROM iceberg.meta_dwh.agg_team_daily
WHERE team_id = 'product-analytics'
  AND usage_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-31';
```

The file also contains weekly and monthly trends, a breakdown of a team's
resources by table, unused tables, and usage of a team's tables by other teams.

## Running

```bash
pip install -r requirements.txt
export TRINO_HOST=trino.internal TRINO_PORT=443 TRINO_HTTP_SCHEME=https TRINO_USER=meta_dwh_loader

python pipeline/run_pipeline.py              # daily run: last 3 days
python pipeline/run_pipeline.py --dry-run    # print SQL only
```

## Possible extensions

* Storage metrics per table from Iceberg metadata tables (`$files`,
  `$snapshots`): table size and growth next to usage.
* Column-level usage from `fct_query_table.accessed_columns`, to find unused
  columns.
* Data quality checks after each load, for example comparing the number of
  queries in `fct_query` with the raw events of the same day.

Author: Javairia Rehman
