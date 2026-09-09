#!/usr/bin/env python3
"""Apply local-only compatibility columns required by production source queries.

The source-replica DDL generator intentionally derives physical columns from
staging sample exports. Some production queries reference source-only predicate
columns that are not selected into staging and therefore cannot appear in those
samples. This script restores those query-only columns in the disposable local
SQL Server replicas without changing the production EL queries.

Currently required:
- editing-management.dbo.Resource_Editings.IsDeleted
- record-survey.dbo.Review.IsDeleted
- record-survey.dbo.User.IsDeleted
- record-survey.dbo.RecordingSoundVersion.IsDeleted

All fixture rows are active records, so missing IsDeleted values default to 0.
The script is idempotent and is intended to run after bootstrap_source_replicas.
"""
from __future__ import annotations

import os

import pyodbc


def env(name: str, default: str = "", required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def main() -> None:
    host = env("SOURCE_MSSQL_ADMIN_HOST", env("EDITING_HOST", "source-mssql"))
    port = int(env("SOURCE_MSSQL_ADMIN_PORT", env("EDITING_PORT", "1433")))
    user = env("SOURCE_MSSQL_ADMIN_USER", "sa")
    password = env("SOURCE_MSSQL_SA_PASSWORD", env("EDITING_PASSWORD"), required=True)

    connection_string = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={host},{port};DATABASE=master;UID={user};PWD={password};"
        "TrustServerCertificate=yes;Encrypt=no;Connection Timeout=30;"
    )

    targets = [
        ("editing-management", "dbo", "Resource_Editings"),
        ("record-survey", "dbo", "Review"),
        ("record-survey", "dbo", "User"),
        ("record-survey", "dbo", "RecordingSoundVersion"),
    ]

    conn = pyodbc.connect(connection_string, autocommit=True)
    try:
        cur = conn.cursor()
        for database, schema, table in targets:
            db = database.replace("]", "]]" )
            schema_table = f"{schema}.{table}".replace("'", "''")
            qschema = schema.replace("]", "]]" )
            qtable = table.replace("]", "]]" )
            constraint = f"DF_local_{table}_IsDeleted".replace("]", "]]" )

            sql = f"""
USE [{db}];
IF COL_LENGTH(N'{schema_table}', N'IsDeleted') IS NULL
BEGIN
    ALTER TABLE [{qschema}].[{qtable}]
        ADD [IsDeleted] BIT NOT NULL
        CONSTRAINT [{constraint}] DEFAULT (0) WITH VALUES;
END;
"""
            cur.execute(sql)
            while cur.nextset():
                pass
            print(f"[local-compat] {database}.{schema}.{table}.IsDeleted: OK")
    finally:
        conn.close()

    print(f"Local source compatibility applied: {len(targets)} query-only columns")


if __name__ == "__main__":
    main()
