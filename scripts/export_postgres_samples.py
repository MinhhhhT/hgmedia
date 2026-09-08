"""Export a limited CSV sample from every table in selected PostgreSQL schemas."""

from __future__ import annotations

import argparse
import csv
import os
import re
from datetime import datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEMAS = ("staging", "silver", "gold")


def safe_filename(value: str) -> str:
    value = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", value).rstrip(". ")
    return value[:180] or "unnamed"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", default="hgmediadb")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--schemas", nargs="+", default=list(DEFAULT_SCHEMAS))
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.limit < 1:
        raise ValueError("--limit must be at least 1")

    load_dotenv(PROJECT_ROOT / ".env")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = args.output_dir or (
        PROJECT_ROOT / "exports" / f"{args.database}_samples_{args.limit}_{timestamp}"
    )
    output_dir.mkdir(parents=True, exist_ok=False)

    connection = psycopg2.connect(
        host=os.getenv("DWH_PG_HOST", "localhost"),
        port=os.getenv("DWH_PG_PORT", "5432"),
        dbname=args.database,
        user=os.environ["DWH_PG_USER"],
        password=os.environ["DWH_PG_PASSWORD"],
        connect_timeout=15,
        application_name="export_postgres_samples",
    )
    connection.set_session(readonly=True, autocommit=False)

    results: list[dict[str, object]] = []
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                  AND table_schema = ANY(%s)
                ORDER BY table_schema, table_name
                """,
                (args.schemas,),
            )
            tables = cursor.fetchall()

        print(f"Database: {args.database}")
        print(f"Found {len(tables)} tables in: {', '.join(args.schemas)}")

        for schema_name, table_name in tables:
            filename = safe_filename(f"{schema_name}.{table_name}") + ".csv"
            output_file = output_dir / filename
            status = "success"
            error = ""
            rows = 0

            try:
                copy_statement = sql.SQL(
                    "COPY (SELECT * FROM {}.{} LIMIT {}) "
                    "TO STDOUT WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')"
                ).format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                    sql.Literal(args.limit),
                )

                with connection.cursor() as cursor, output_file.open(
                    "w", encoding="utf-8", newline=""
                ) as csv_file:
                    cursor.copy_expert(copy_statement.as_string(connection), csv_file)
                    rows = max(cursor.rowcount, 0)
                connection.commit()
                print(f"OK    {schema_name}.{table_name}: {rows} rows")
            except Exception as exc:  # Continue exporting the remaining tables.
                connection.rollback()
                output_file.unlink(missing_ok=True)
                status = "error"
                error = str(exc).replace("\r", " ").replace("\n", " ")
                print(f"ERROR {schema_name}.{table_name}: {error}")

            results.append(
                {
                    "schema": schema_name,
                    "table": table_name,
                    "rows": rows,
                    "status": status,
                    "file": filename if status == "success" else "",
                    "error": error,
                }
            )
    finally:
        connection.close()

    manifest = output_dir / "manifest.csv"
    with manifest.open("w", encoding="utf-8-sig", newline="") as manifest_file:
        writer = csv.DictWriter(
            manifest_file,
            fieldnames=("schema", "table", "rows", "status", "file", "error"),
        )
        writer.writeheader()
        writer.writerows(results)

    succeeded = sum(item["status"] == "success" for item in results)
    failed = len(results) - succeeded
    print(f"Completed: {succeeded} succeeded, {failed} failed")
    print(f"Output: {output_dir}")


if __name__ == "__main__":
    main()
