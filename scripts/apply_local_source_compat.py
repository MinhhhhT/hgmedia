#!/usr/bin/env python3
"""Apply local-only compatibility required by production source queries/models.

The source-replica DDL generator derives physical columns from staging sample
exports plus Data Dictionary metadata. A few source-only predicate columns are
not present in staging samples, and some application enum types are represented
by custom dictionary names even though SQL Server stores numeric enum values.

This script restores those local-replica details without changing production EL
queries or dbt business logic.

Currently required:
- editing-management.dbo.Resource_Editings.IsDeleted
- record-survey.dbo.Review.IsDeleted
- record-survey.dbo.User.IsDeleted
- record-survey.dbo.RecordingSoundVersion.IsDeleted
- editing-management.dbo.Resources.ResourceType -> BIGINT

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


def _drain(cur) -> None:
    while cur.nextset():
        pass


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

    soft_delete_targets = [
        ("editing-management", "dbo", "Resource_Editings"),
        ("record-survey", "dbo", "Review"),
        ("record-survey", "dbo", "User"),
        ("record-survey", "dbo", "RecordingSoundVersion"),
    ]

    conn = pyodbc.connect(connection_string, autocommit=True)
    try:
        cur = conn.cursor()

        for database, schema, table in soft_delete_targets:
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
            _drain(cur)
            print(f"[local-compat] {database}.{schema}.{table}.IsDeleted: OK")

        # The Editing application exposes ResourceType as an enum. The staging
        # sample contains numeric values (0/2), and the HG dbt model compares it
        # numerically. Older local DDL generation treated the custom enum name
        # as NVARCHAR, which changed source semantics and produced text in
        # staging. Convert the disposable replica back to the numeric form.
        sql = """
USE [editing-management];
IF OBJECT_ID(N'dbo.Resources', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.Resources', N'ResourceType') IS NOT NULL
   AND EXISTS (
       SELECT 1
       FROM sys.columns c
       JOIN sys.types t ON c.user_type_id = t.user_type_id
       WHERE c.object_id = OBJECT_ID(N'dbo.Resources')
         AND c.name = N'ResourceType'
         AND t.name IN (N'nvarchar', N'varchar', N'nchar', N'char', N'ntext', N'text')
   )
BEGIN
    IF EXISTS (
        SELECT 1
        FROM dbo.Resources
        WHERE NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(100), ResourceType))), N'') IS NOT NULL
          AND TRY_CONVERT(bigint, ResourceType) IS NULL
    )
        THROW 51000, 'Local Resources.ResourceType contains non-numeric values; refusing conversion.', 1;

    UPDATE dbo.Resources
       SET ResourceType = NULL
     WHERE NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(100), ResourceType))), N'') IS NULL;

    ALTER TABLE dbo.Resources ALTER COLUMN ResourceType BIGINT NULL;
END;
"""
        cur.execute(sql)
        _drain(cur)
        print("[local-compat] editing-management.dbo.Resources.ResourceType: BIGINT OK")
    finally:
        conn.close()

    print(
        "Local source compatibility applied: "
        f"{len(soft_delete_targets)} query-only columns + 1 enum type alignment"
    )


if __name__ == "__main__":
    main()
