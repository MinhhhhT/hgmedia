#!/usr/bin/env python3
"""Apply local-only compatibility required by production source queries/models.

The source-replica DDL generator derives physical columns from staging sample
exports plus Data Dictionary metadata. A few source-only predicate columns are
not present in staging samples, some application enum types are represented by
custom dictionary names, and independent per-table samples can lose relational
overlap that exists in production.

This script restores those local-replica details without changing production EL
queries or dbt business logic. It only modifies the disposable local SQL Server
replica used by docker-compose.rebuild.yml.

Currently required:
- editing-management.dbo.Resource_Editings.IsDeleted
- record-survey.dbo.Review.IsDeleted
- record-survey.dbo.User.IsDeleted
- record-survey.dbo.RecordingSoundVersion.IsDeleted
- editing-management.dbo.Resources.ResourceType -> BIGINT
- deterministic Editing/Resource/Resource_Editings overlap for local fixtures
- deterministic EditingFileId/channel_video_info.Code overlap for bridge_bt_vid

All fixture rows are active records, so missing IsDeleted values default to 0.
Relationship repair reuses existing sampled rows and only rewires a small,
deterministic subset. The script is idempotent and is intended to run after
bootstrap_source_replicas.
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pyodbc

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EXPORT_DIR = REPO_ROOT / "exports" / "hgmediadb_samples_1000_20260828_000045"
DEFAULT_RELATION_COUNT = 25


def env(name: str, default: str = "", required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _drain(cur) -> None:
    while cur.nextset():
        pass


def _resolve_fixture_dir() -> Path:
    raw = env("LOCAL_FIXTURE_DIR", str(DEFAULT_EXPORT_DIR))
    path = Path(raw)
    if not path.is_absolute():
        path = REPO_ROOT / path
    return path.resolve()


def _load_channel_codes() -> list[str]:
    path = _resolve_fixture_dir() / "staging.channel_video_info.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing channel video fixture: {path}")

    df = pd.read_csv(path, dtype=str, keep_default_na=False, low_memory=False)
    required = {"Code", "YoutubeVideoId"}
    missing = required.difference(df.columns)
    if missing:
        raise RuntimeError(
            f"channel_video_info fixture missing columns: {sorted(missing)}"
        )

    seen: set[str] = set()
    codes: list[str] = []
    for code, video_id in zip(df["Code"], df["YoutubeVideoId"]):
        code = str(code).strip()
        video_id = str(video_id).strip()
        if not code or not video_id or code in seen:
            continue
        seen.add(code)
        codes.append(code)
    return codes


def _load_excluded_resource_codes() -> set[str]:
    path = REPO_ROOT / "dwh_dbt" / "seeds" / "manual_excluded_stock_codes.csv"
    if not path.exists():
        return set()

    df = pd.read_csv(path, dtype=str, keep_default_na=False, low_memory=False)
    if "hg_stock_id" not in df.columns:
        return set()

    return {
        str(value).strip().upper()
        for value in df["hg_stock_id"]
        if str(value).strip()
    }


def _repair_editing_relationships(cur) -> int:
    """Create deterministic FK/business-key overlap in local editing fixtures."""
    desired = int(env("LOCAL_FIXTURE_RELATION_COUNT", str(DEFAULT_RELATION_COUNT)))
    channel_codes = _load_channel_codes()
    excluded_codes = _load_excluded_resource_codes()

    if not channel_codes:
        raise RuntimeError("No usable Code/YoutubeVideoId pairs in channel_video_info fixture")

    cur.execute("USE [editing-management]")
    _drain(cur)

    editings = [
        str(row[0])
        for row in cur.execute(
            "SELECT TOP (200) CAST([Id] AS NVARCHAR(36)) "
            "FROM [dbo].[Editings] WHERE [Id] IS NOT NULL ORDER BY [Id]"
        ).fetchall()
    ]

    resource_rows = cur.execute(
        "SELECT TOP (1000) CAST([Id] AS NVARCHAR(36)), "
        "CAST([ResourceFileId] AS NVARCHAR(255)) "
        "FROM [dbo].[Resources] "
        "WHERE [ResourceType] = 0 "
        "AND UPPER(LTRIM(RTRIM([ResourceFileId]))) LIKE 'HGFA%' "
        "ORDER BY [Id]"
    ).fetchall()
    resources = [
        (str(row[0]), str(row[1]).strip())
        for row in resource_rows
        if str(row[1]).strip().upper() not in excluded_codes
    ]

    relation_rows = [
        str(row[0])
        for row in cur.execute(
            "SELECT TOP (200) CAST([Id] AS NVARCHAR(36)) "
            "FROM [dbo].[Resource_Editings] WHERE [Id] IS NOT NULL ORDER BY [Id]"
        ).fetchall()
    ]

    count = min(desired, len(channel_codes), len(editings), len(resources), len(relation_rows))
    if count <= 0:
        raise RuntimeError(
            "Unable to create local editing overlap: "
            f"channel_codes={len(channel_codes)}, editings={len(editings)}, "
            f"resources={len(resources)}, resource_editings={len(relation_rows)}"
        )

    for index in range(count):
        editing_id = editings[index]
        resource_id, _resource_code = resources[index]
        relation_id = relation_rows[index]
        editing_code = channel_codes[index]

        cur.execute(
            "UPDATE [dbo].[Editings] SET [EditingFileId] = ? WHERE [Id] = ?",
            editing_code,
            editing_id,
        )
        cur.execute(
            "UPDATE [dbo].[Resource_Editings] "
            "SET [EditingId] = ?, [ResourcesId] = ? WHERE [Id] = ?",
            editing_id,
            resource_id,
            relation_id,
        )

    print(
        "[local-compat] editing lineage overlap: "
        f"{count} Resource_Editings rows aligned to sampled Editings/Resources"
    )
    print(
        "[local-compat] editing/video overlap: "
        f"{count} EditingFileId values aligned to channel_video_info.Code"
    )
    return count


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

        repaired = _repair_editing_relationships(cur)
    finally:
        conn.close()

    print(
        "Local source compatibility applied: "
        f"{len(soft_delete_targets)} query-only columns + 1 enum type alignment "
        f"+ {repaired} deterministic editing relationships"
    )


if __name__ == "__main__":
    main()
