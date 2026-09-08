"""Create a restorable plain-SQL PostgreSQL dump with limited rows per table.

This is intended for development/sample databases. Foreign-key constraints are
restored as NOT VALID because independently sampled tables may omit parent rows.
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", default="hgmediadb")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--schemas", nargs="+", default=["staging", "silver", "gold"])
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def ident(connection, *parts: str) -> str:
    return sql.SQL(".").join(sql.Identifier(part) for part in parts).as_string(connection)


def literal(connection, value: object) -> str:
    return sql.Literal(value).as_string(connection)


def write_line(output, value: str = "") -> None:
    output.write(value)
    output.write("\n")


def main() -> None:
    args = parse_args()
    if args.limit < 1:
        raise ValueError("--limit must be at least 1")

    load_dotenv(PROJECT_ROOT / ".env")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = args.output or (
        PROJECT_ROOT
        / "exports"
        / f"{args.database}_{'_'.join(args.schemas)}_limited_{args.limit}_{timestamp}.sql"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        raise FileExistsError(output_path)

    connection = psycopg2.connect(
        host=os.getenv("DWH_PG_HOST", "localhost"),
        port=os.getenv("DWH_PG_PORT", "5432"),
        dbname=args.database,
        user=os.environ["DWH_PG_USER"],
        password=os.environ["DWH_PG_PASSWORD"],
        connect_timeout=15,
        application_name="export_postgres_limited_dump",
    )
    connection.set_session(readonly=True, autocommit=False)

    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW server_version")
            server_version = cursor.fetchone()[0]
            cursor.execute(
                """
                SELECT c.oid, n.nspname, c.relname, c.relpersistence, c.relkind
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ANY(%s)
                  AND c.relkind IN ('r', 'p')
                  AND NOT c.relispartition
                ORDER BY n.nspname, c.relname
                """,
                (args.schemas,),
            )
            tables = cursor.fetchall()
            cursor.execute(
                """
                SELECT n.nspname, c.relname, c.relkind
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ANY(%s)
                  AND (c.relispartition OR c.relkind = 'p')
                ORDER BY n.nspname, c.relname
                """,
                (args.schemas,),
            )
            partitioned_objects = cursor.fetchall()

        if partitioned_objects:
            names = ", ".join(f"{schema}.{name}" for schema, name, _ in partitioned_objects)
            raise RuntimeError(f"Partitioned tables are not supported by this exporter: {names}")

        print(f"Database: {args.database} (PostgreSQL {server_version})")
        print(f"Writing {len(tables)} tables to: {output_path}")

        with output_path.open("w", encoding="utf-8", newline="\n") as output:
            write_line(output, "-- PostgreSQL limited sample dump")
            write_line(output, f"-- Source database: {args.database}")
            write_line(output, f"-- Schemas: {', '.join(args.schemas)}")
            write_line(output, f"-- Maximum rows per table: {args.limit}")
            write_line(output, "-- Restore with: psql -X -v ON_ERROR_STOP=1 -d TARGET_DB -f THIS_FILE.sql")
            write_line(output, "-- Foreign keys are restored NOT VALID because samples may omit parent rows.")
            write_line(output)
            write_line(output, "SET client_encoding = 'UTF8';")
            write_line(output, "SET standard_conforming_strings = on;")
            write_line(output, "SET check_function_bodies = false;")
            write_line(output, "SET client_min_messages = warning;")
            write_line(output, "SET row_security = off;")
            write_line(output)

            for schema_name in args.schemas:
                write_line(
                    output,
                    f"CREATE SCHEMA IF NOT EXISTS {ident(connection, schema_name)};",
                )
            write_line(output)

            # Enum types used by the selected tables.
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT tn.nspname, t.typname, t.oid
                    FROM pg_catalog.pg_attribute a
                    JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    JOIN pg_catalog.pg_type raw_t ON raw_t.oid = a.atttypid
                    JOIN pg_catalog.pg_type t
                      ON t.oid = CASE
                          WHEN raw_t.typelem <> 0 THEN raw_t.typelem
                          ELSE raw_t.oid
                      END
                    JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace
                    WHERE n.nspname = ANY(%s)
                      AND c.relkind IN ('r', 'p')
                      AND a.attnum > 0
                      AND NOT a.attisdropped
                      AND t.typtype = 'e'
                    ORDER BY tn.nspname, t.typname, t.oid
                    """,
                    (args.schemas,),
                )
                enums = cursor.fetchall()
                for enum_schema, enum_name, enum_oid in enums:
                    cursor.execute(
                        "SELECT enumlabel FROM pg_catalog.pg_enum "
                        "WHERE enumtypid = %s ORDER BY enumsortorder",
                        (enum_oid,),
                    )
                    labels = ", ".join(literal(connection, row[0]) for row in cursor.fetchall())
                    write_line(
                        output,
                        f"CREATE TYPE {ident(connection, enum_schema, enum_name)} AS ENUM ({labels});",
                    )
            if enums:
                write_line(output)

            # Standalone/serial sequences. Identity sequences are created with their columns.
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT c.oid, n.nspname, c.relname, s.seqstart, s.seqincrement,
                           s.seqmin, s.seqmax, s.seqcache, s.seqcycle
                    FROM pg_catalog.pg_class c
                    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    JOIN pg_catalog.pg_sequence s ON s.seqrelid = c.oid
                    WHERE c.relkind = 'S'
                      AND n.nspname = ANY(%s)
                      AND NOT EXISTS (
                          SELECT 1 FROM pg_catalog.pg_depend d
                          WHERE d.objid = c.oid AND d.deptype = 'i'
                      )
                    ORDER BY n.nspname, c.relname
                    """,
                    (args.schemas,),
                )
                sequences = cursor.fetchall()
                for _, seq_schema, seq_name, start, increment, minimum, maximum, cache, cycle in sequences:
                    cycle_sql = "CYCLE" if cycle else "NO CYCLE"
                    write_line(
                        output,
                        f"CREATE SEQUENCE {ident(connection, seq_schema, seq_name)} "
                        f"START WITH {start} INCREMENT BY {increment} MINVALUE {minimum} "
                        f"MAXVALUE {maximum} CACHE {cache} {cycle_sql};",
                    )
            if sequences:
                write_line(output)

            table_columns: dict[int, list[tuple]] = {}
            for table_oid, schema_name, table_name, persistence, _ in tables:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT a.attname,
                               pg_catalog.format_type(a.atttypid, a.atttypmod),
                               a.attnotnull,
                               pg_catalog.pg_get_expr(ad.adbin, ad.adrelid),
                               a.attidentity,
                               a.attgenerated
                        FROM pg_catalog.pg_attribute a
                        LEFT JOIN pg_catalog.pg_attrdef ad
                          ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
                        WHERE a.attrelid = %s
                          AND a.attnum > 0
                          AND NOT a.attisdropped
                        ORDER BY a.attnum
                        """,
                        (table_oid,),
                    )
                    columns = cursor.fetchall()
                    table_columns[table_oid] = columns

                column_definitions = []
                for name, data_type, not_null, default, identity, generated in columns:
                    definition = f"    {ident(connection, name)} {data_type}"
                    if generated:
                        definition += f" GENERATED ALWAYS AS ({default}) STORED"
                    elif identity:
                        generation = "ALWAYS" if identity == "a" else "BY DEFAULT"
                        definition += f" GENERATED {generation} AS IDENTITY"
                    elif default is not None:
                        definition += f" DEFAULT {default}"
                    if not_null:
                        definition += " NOT NULL"
                    column_definitions.append(definition)

                table_kind = "UNLOGGED TABLE" if persistence == "u" else "TABLE"
                write_line(
                    output,
                    f"CREATE {table_kind} {ident(connection, schema_name, table_name)} (",
                )
                write_line(output, ",\n".join(column_definitions))
                write_line(output, ");")
            write_line(output)

            # Restore serial sequence ownership after tables exist.
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT sn.nspname, seq.relname, tn.nspname, tbl.relname, a.attname
                    FROM pg_catalog.pg_class seq
                    JOIN pg_catalog.pg_namespace sn ON sn.oid = seq.relnamespace
                    JOIN pg_catalog.pg_depend d ON d.objid = seq.oid AND d.deptype = 'a'
                    JOIN pg_catalog.pg_class tbl ON tbl.oid = d.refobjid
                    JOIN pg_catalog.pg_namespace tn ON tn.oid = tbl.relnamespace
                    JOIN pg_catalog.pg_attribute a
                      ON a.attrelid = tbl.oid AND a.attnum = d.refobjsubid
                    WHERE seq.relkind = 'S' AND sn.nspname = ANY(%s)
                    ORDER BY sn.nspname, seq.relname
                    """,
                    (args.schemas,),
                )
                for seq_schema, seq_name, table_schema, table_name, column_name in cursor.fetchall():
                    write_line(
                        output,
                        f"ALTER SEQUENCE {ident(connection, seq_schema, seq_name)} OWNED BY "
                        f"{ident(connection, table_schema, table_name)}.{ident(connection, column_name)};",
                    )
            write_line(output)

            total_rows = 0
            for table_oid, schema_name, table_name, _, _ in tables:
                insertable_columns = [
                    name for name, _, _, _, _, generated in table_columns[table_oid] if not generated
                ]
                if not insertable_columns:
                    continue
                column_sql = sql.SQL(", ").join(
                    sql.Identifier(name) for name in insertable_columns
                ).as_string(connection)
                qualified_table = ident(connection, schema_name, table_name)
                write_line(output, f"COPY {qualified_table} ({column_sql}) FROM stdin;")
                copy_out = sql.SQL("COPY (SELECT {} FROM {}.{} LIMIT {}) TO STDOUT").format(
                    sql.SQL(", ").join(sql.Identifier(name) for name in insertable_columns),
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                    sql.Literal(args.limit),
                )
                with connection.cursor() as cursor:
                    cursor.copy_expert(copy_out.as_string(connection), output)
                    row_count = max(cursor.rowcount, 0)
                write_line(output, "\\.")
                write_line(output)
                total_rows += row_count
                print(f"DATA  {schema_name}.{table_name}: {row_count} rows")

            # Primary and unique constraints must exist before foreign keys.
            for constraint_types in (("p", "u"), ("x",), ("c",), ("f",)):
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT n.nspname, c.relname, con.conname, con.contype,
                               pg_catalog.pg_get_constraintdef(con.oid, true)
                        FROM pg_catalog.pg_constraint con
                        JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
                        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = ANY(%s)
                          AND con.contype::text = ANY(%s)
                        ORDER BY n.nspname, c.relname, con.conname
                        """,
                        (args.schemas, list(constraint_types)),
                    )
                    for schema_name, table_name, name, kind, definition in cursor.fetchall():
                        if kind in ("f", "c") and "NOT VALID" not in definition.upper():
                            definition += " NOT VALID"
                        write_line(
                            output,
                            f"ALTER TABLE ONLY {ident(connection, schema_name, table_name)} "
                            f"ADD CONSTRAINT {ident(connection, name)} {definition};",
                        )
            write_line(output)

            # Non-constraint indexes.
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT pg_catalog.pg_get_indexdef(i.indexrelid)
                    FROM pg_catalog.pg_index i
                    JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
                    JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
                    LEFT JOIN pg_catalog.pg_constraint con ON con.conindid = i.indexrelid
                    WHERE n.nspname = ANY(%s)
                      AND con.oid IS NULL
                    ORDER BY n.nspname, t.relname, i.indexrelid
                    """,
                    (args.schemas,),
                )
                for (index_definition,) in cursor.fetchall():
                    write_line(output, index_definition + ";")
            write_line(output)

            # Preserve current sequence counters, including identity sequences.
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT n.nspname, c.relname
                    FROM pg_catalog.pg_class c
                    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relkind = 'S' AND n.nspname = ANY(%s)
                    ORDER BY n.nspname, c.relname
                    """,
                    (args.schemas,),
                )
                all_sequences = cursor.fetchall()
                for seq_schema, seq_name in all_sequences:
                    sequence_name = ident(connection, seq_schema, seq_name)
                    cursor.execute(f"SELECT last_value, is_called FROM {sequence_name}")
                    last_value, is_called = cursor.fetchone()
                    write_line(
                        output,
                        "SELECT pg_catalog.setval("
                        f"{literal(connection, sequence_name)}::regclass, {last_value}, "
                        f"{'true' if is_called else 'false'});",
                    )

            write_line(output)
            write_line(output, "-- Dump completed successfully.")

        connection.commit()
        print(f"Completed: {len(tables)} tables, {total_rows} rows")
        print(f"Output: {output_path}")
    except Exception:
        output_path.unlink(missing_ok=True)
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    main()
