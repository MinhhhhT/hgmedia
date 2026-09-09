#!/usr/bin/env python3
"""Restore relational overlap that independent 1000-row source samples cannot preserve.

The exported staging CSV files were sampled table-by-table. For Editing Management,
that means Resource_Editings.EditingId / ResourcesId may not point to rows that
happen to be present in the separately sampled Editings and Resources fixtures.
The production dbt model is correct, but a clean local rebuild would therefore
produce an empty silver.fact_editing.

Use the exported silver.fact_editing sample as a *reference fixture* and append a
small deterministic set of relationally coherent rows to the disposable local
Editing Management source replica. The real EL query and dbt transformations are
left unchanged; running them must recreate the relationships themselves.

The repair is idempotent:
- existing Editings rows are updated/inserted by their real editing_id;
- synthetic Resources IDs are deterministic UUIDv5 values derived from hg_stock_id;
- synthetic Resource_Editings IDs are deterministic UUIDv5 values derived from
  fact_editing_sk.
"""
from __future__ import annotations

import os
import uuid
from pathlib import Path

import pandas as pd
import pyodbc

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REFERENCE = (
    REPO_ROOT
    / "exports"
    / "hgmediadb_samples_1000_20260828_000045"
    / "silver.fact_editing.csv"
)
UUID_NAMESPACE = uuid.UUID("c49f1e9b-a202-4c08-9668-707322eadf6e")


def env(name: str, default: str = "", required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def deterministic_uuid(kind: str, key: str) -> str:
    return str(uuid.uuid5(UUID_NAMESPACE, f"{kind}:{key}"))


def main() -> None:
    reference_path = Path(env("LOCAL_FACT_EDITING_REFERENCE", str(DEFAULT_REFERENCE)))
    if not reference_path.is_absolute():
        reference_path = REPO_ROOT / reference_path
    if not reference_path.exists():
        raise FileNotFoundError(f"Local relational reference fixture not found: {reference_path}")

    df = pd.read_csv(reference_path, dtype=str, keep_default_na=False, low_memory=False)
    required = {
        "fact_editing_sk", "editing_id", "editing_code", "hg_stock_id", "position", "duration"
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise RuntimeError(f"Reference fixture missing columns: {missing}")

    df = df[list(required)].copy()
    for col in ("fact_editing_sk", "editing_id", "editing_code", "hg_stock_id"):
        df[col] = df[col].astype(str).str.strip()
    df = df[
        (df["fact_editing_sk"] != "")
        & (df["editing_id"] != "")
        & (df["editing_code"] != "")
        & (df["hg_stock_id"] != "")
    ].copy()
    df["position_num"] = pd.to_numeric(df["position"], errors="coerce")
    df["duration_num"] = pd.to_numeric(df["duration"], errors="coerce")
    df = df[df["position_num"].notna() & df["duration_num"].notna()].copy()
    df["position_num"] = df["position_num"].astype(int)
    df["duration_num"] = df["duration_num"].round().astype(int)

    max_rows = int(env("LOCAL_RELATIONAL_FIXTURE_ROWS", "1000"))
    if max_rows > 0:
        df = df.head(max_rows).copy()
    if df.empty:
        raise RuntimeError("Reference fixture contains no usable fact_editing rows")

    host = env("SOURCE_MSSQL_ADMIN_HOST", env("EDITING_HOST", "source-mssql"))
    port = int(env("SOURCE_MSSQL_ADMIN_PORT", env("EDITING_PORT", "1433")))
    user = env("SOURCE_MSSQL_ADMIN_USER", "sa")
    password = env("SOURCE_MSSQL_SA_PASSWORD", env("EDITING_PASSWORD"), required=True)

    connection_string = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={host},{port};DATABASE=editing-management;UID={user};PWD={password};"
        "TrustServerCertificate=yes;Encrypt=no;Connection Timeout=30;"
    )
    conn = pyodbc.connect(connection_string, autocommit=False)
    try:
        cur = conn.cursor()

        # Keep one canonical editing_code for each editing_id from the reference sample.
        editing_rows = (
            df[["editing_id", "editing_code"]]
            .drop_duplicates(subset=["editing_id"], keep="first")
            .itertuples(index=False, name=None)
        )
        editing_count = 0
        for editing_id, editing_code in editing_rows:
            cur.execute(
                """
UPDATE dbo.Editings SET EditingFileId = ? WHERE Id = ?;
IF @@ROWCOUNT = 0
    INSERT INTO dbo.Editings (Id, EditingFileId) VALUES (?, ?);
""",
                editing_code, editing_id, editing_id, editing_code,
            )
            editing_count += 1

        resource_map: dict[str, str] = {}
        resource_count = 0
        for hg_stock_id in df["hg_stock_id"].drop_duplicates().tolist():
            resource_id = deterministic_uuid("resource", hg_stock_id)
            resource_map[hg_stock_id] = resource_id
            cur.execute(
                """
UPDATE dbo.Resources
SET ResourceFileId = ?, ResourceType = 0
WHERE Id = ?;
IF @@ROWCOUNT = 0
    INSERT INTO dbo.Resources (Id, ResourceFileId, ResourceType)
    VALUES (?, ?, 0);
""",
                hg_stock_id, resource_id, resource_id, hg_stock_id,
            )
            resource_count += 1

        relation_count = 0
        fixed_created_date = "2026-08-28T00:00:00"
        for row in df.itertuples(index=False):
            relation_id = deterministic_uuid("resource_editing", row.fact_editing_sk)
            resource_id = resource_map[row.hg_stock_id]
            # Use a wide deterministic stride so row_number() ordered by StartTime
            # reproduces the reference position within each editing.
            start_time = int(row.position_num) * 10_000_000
            end_time = start_time + int(row.duration_num)
            cur.execute(
                """
UPDATE dbo.Resource_Editings
SET EditingId = ?, ResourcesId = ?, StartTime = ?, EndTime = ?,
    CreatedDate = ?, UpdatedDate = NULL, IsDeleted = 0
WHERE Id = ?;
IF @@ROWCOUNT = 0
    INSERT INTO dbo.Resource_Editings
        (Id, EditingId, ResourcesId, StartTime, EndTime, CreatedDate, UpdatedDate, IsDeleted)
    VALUES (?, ?, ?, ?, ?, ?, NULL, 0);
""",
                row.editing_id,
                resource_id,
                start_time,
                end_time,
                fixed_created_date,
                relation_id,
                relation_id,
                row.editing_id,
                resource_id,
                start_time,
                end_time,
                fixed_created_date,
            )
            relation_count += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(
        "[local-relational-fixture] Editing Management overlap restored: "
        f"{editing_count} editings, {resource_count} resources, "
        f"{relation_count} resource_editings"
    )
    print(f"[local-relational-fixture] reference: {reference_path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
