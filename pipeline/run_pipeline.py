"""
Runs the Meta DWH load against Trino.

Daily run (default): reloads the last LOOKBACK_DAYS days up to yesterday,
which also picks up late-arriving events.

    python pipeline/run_pipeline.py

Backfill or recalculation of a period, processed in chunks:

    python pipeline/run_pipeline.py --start-date 2026-01-01 --end-date 2026-06-30 --chunk-days 7

Print the rendered SQL without executing it:

    python pipeline/run_pipeline.py --dry-run

Connection settings come from environment variables:
TRINO_HOST, TRINO_PORT, TRINO_USER, TRINO_HTTP_SCHEME.
"""

import argparse
import logging
import os
import re
from datetime import date, timedelta
from pathlib import Path

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
LOOKBACK_DAYS = 3

# Run once per pipeline run (DDL and full-rebuild dimensions).
SETUP_SCRIPTS = [
    "01_ddl.sql",
    "02_dim_team.sql",
    "03_dim_user_team.sql",
]

# Run for every date chunk, in this order.
INCREMENTAL_SCRIPTS = [
    "04_fct_query.sql",
    "05_fct_query_table.sql",
    "06_dim_table.sql",
    "07_agg_table_user_daily.sql",
    "08_agg_team_daily.sql",
]

log = logging.getLogger("meta_dwh")


def parse_args():
    yesterday = date.today() - timedelta(days=1)
    parser = argparse.ArgumentParser(description="Load the Meta DWH")
    parser.add_argument("--start-date", type=date.fromisoformat,
                        default=yesterday - timedelta(days=LOOKBACK_DAYS - 1))
    parser.add_argument("--end-date", type=date.fromisoformat, default=yesterday)
    parser.add_argument("--chunk-days", type=int, default=7,
                        help="number of days processed per chunk")
    parser.add_argument("--skip-setup", action="store_true",
                        help="do not run DDL and dimension rebuilds")
    parser.add_argument("--dry-run", action="store_true",
                        help="print SQL instead of executing it")
    args = parser.parse_args()
    if args.start_date > args.end_date:
        parser.error("--start-date must not be after --end-date")
    if args.chunk_days < 1:
        parser.error("--chunk-days must be at least 1")
    return args


def date_chunks(start, end, chunk_days):
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def render(sql, start=None, end=None):
    if start is not None:
        sql = sql.replace("{{start_date}}", f"DATE '{start.isoformat()}'")
        sql = sql.replace("{{end_date}}", f"DATE '{end.isoformat()}'")
    return sql


def split_statements(sql):
    """Split a script into statements. Comment lines are removed first,
    statements are separated by semicolons."""
    without_comments = "\n".join(
        line for line in sql.splitlines() if not line.strip().startswith("--")
    )
    statements = [s.strip() for s in without_comments.split(";")]
    return [s for s in statements if s]


class Runner:
    def __init__(self, dry_run):
        self.dry_run = dry_run
        self.cursor = None
        if not dry_run:
            import trino  # imported here so --dry-run works without the client

            conn = trino.dbapi.connect(
                host=os.environ.get("TRINO_HOST", "localhost"),
                port=int(os.environ.get("TRINO_PORT", "8080")),
                user=os.environ.get("TRINO_USER", "meta_dwh_loader"),
                http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
                catalog="iceberg",
                schema="meta_dwh",
            )
            self.cursor = conn.cursor()

    def run_script(self, name, start=None, end=None):
        sql = render((SQL_DIR / name).read_text(), start, end)
        if "{{" in sql:
            raise ValueError(f"{name} has unrendered parameters")
        for statement in split_statements(sql):
            first_line = re.sub(r"\s+", " ", statement)[:80]
            log.info("%s: %s ...", name, first_line)
            if self.dry_run:
                print(statement + ";\n")
            else:
                self.cursor.execute(statement)
                self.cursor.fetchall()  # wait until the statement finishes


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    runner = Runner(args.dry_run)

    if not args.skip_setup:
        for script in SETUP_SCRIPTS:
            runner.run_script(script)

    for start, end in date_chunks(args.start_date, args.end_date, args.chunk_days):
        log.info("Loading %s .. %s", start, end)
        for script in INCREMENTAL_SCRIPTS:
            runner.run_script(script, start, end)

    log.info("Done")


if __name__ == "__main__":
    main()
