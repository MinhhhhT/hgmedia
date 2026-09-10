#!/usr/bin/env python3
"""Restore relational overlap that independent source samples cannot preserve.

The exported staging CSV files were sampled table-by-table, so child rows can
reference parents that are absent from another independently sampled fixture.
This script repairs only the disposable local source replicas; production EL and
dbt business logic are left unchanged.

Repairs currently applied:
- Editing Management: restore Editings / Resources / Resource_Editings overlap
  from the exported silver.fact_editing reference fixture.
- Odoo purchase orders: restore purchase_order parents referenced by sampled
  purchase_order_line rows. Prefer the exported silver.dim_po reference because
  dim_po is a direct projection of purchase_order. If a parent is absent from
  that reference too, use the earliest sampled purchase_order_line.create_date
  as a local-only cohort proxy and preserve company/state/user evidence from the
  child rows. The proxy is not claimed to be the real PO header timestamp.

The repair is idempotent: existing source rows are updated in place and missing
rows are inserted with stable source identifiers.
"""
from __future__ import annotations

import os
import uuid
from pathlib import Path

import pandas as pd
import psycopg2
import pyodbc

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FACT_EDITING_REFERENCE = (
    REPO_ROOT
    / "exports"
    / "hgmediadb_samples_1000_20260828_000045"
    / "silver.fact_editing.csv"
)
DEFAULT_DIM_PO_REFERENCE = (
    REPO_ROOT
    / "exports"
    / "hgmediadb_samples_1000_20260828_000045"
    / "silver.dim_po.csv"
)
UUID_NAMESPACE = uuid.UUID("c49f1e9b-a202-4c08-9668-707322eadf6e")


def env(name: str, default: str = "", required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def deterministic_uuid(kind: str, key: str) -> str:
    return str(uuid.uuid5(UUID_NAMESPACE, f"{kind}:{key}"))


def reference_path(env_name: str, default: Path) -> Path:
    path = Path(env(env_name, str(default)))
    if not path.is_absolute():
        path = REPO_ROOT / path
    if not path.exists():
        raise FileNotFoundError(f"Local relational reference fixture not found: {path}")
    return path


def none_if_blank(value):
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def restore_odoo_purchase_order_parents() -> None:
    """Restore missing PO headers referenced by sampled purchase_order_line rows."""
    dim_po_path = reference_path("LOCAL_DIM_PO_REFERENCE", DEFAULT_DIM_PO_REFERENCE)
    ref = pd.read_csv(dim_po_path, dtype=str, keep_default_na=False, low_memory=False)
    required = {
        "po_id",
        "po_created_date",
        "order_employee_id",
        "ordering_company",
        "status",
        "po_confirmed_date",
        "po_name",
    }
    missing = sorted(required - set(ref.columns))
    if missing:
        raise RuntimeError(f"dim_po reference fixture missing columns: {missing}")

    ref = ref[list(required)].copy()
    ref["po_id"] = ref["po_id"].astype(str).str.strip()
    ref = ref[ref["po_id"] != ""].drop_duplicates(subset=["po_id"], keep="first")
    reference_by_po = {row.po_id: row for row in ref.itertuples(index=False)}

    host = env("ODOO_PG_HOST", env("SOURCE_PG_ADMIN_HOST", "source-postgres"))
    port = int(env("ODOO_PG_PORT", env("SOURCE_PG_ADMIN_PORT", "5432")))
    database = env("ODOO_PG_DB", "odoo")
    user = env("ODOO_PG_USER", env("SOURCE_PG_ADMIN_USER", "hgsource"))
    password = env(
        "ODOO_PG_PASSWORD",
        env("SOURCE_PG_ADMIN_PASSWORD"),
        required=True,
    )

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password,
        connect_timeout=30,
    )
    exact_count = 0
    proxy_count = 0
    unresolved_count = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    CAST(pol.order_id AS text) AS po_id,
                    MIN(pol.create_date) AS line_created_date,
                    MIN(pol.create_uid) AS line_create_uid,
                    MIN(pol.company_id) AS line_company_id,
                    MIN(pol.state) AS line_state
                FROM public.purchase_order_line pol
                LEFT JOIN public.purchase_order po
                    ON po.id = pol.order_id
                WHERE pol.order_id IS NOT NULL
                  AND po.id IS NULL
                GROUP BY pol.order_id
                ORDER BY pol.order_id
                """
            )
            missing_parents = cur.fetchall()

            for po_id, line_created_date, line_create_uid, line_company_id, line_state in missing_parents:
                ref_row = reference_by_po.get(str(po_id))
                if ref_row is not None:
                    create_date = none_if_blank(ref_row.po_created_date)
                    user_id = none_if_blank(ref_row.order_employee_id)
                    company_id = none_if_blank(ref_row.ordering_company)
                    state = none_if_blank(ref_row.status)
                    date_approve = none_if_blank(ref_row.po_confirmed_date)
                    name = none_if_blank(ref_row.po_name)
                    exact_count += 1
                else:
                    # Local-only fallback. The child timestamp is real sampled source
                    # evidence, but it is only a proxy for the missing header date.
                    create_date = line_created_date
                    user_id = line_create_uid
                    company_id = line_company_id
                    state = line_state
                    date_approve = None
                    name = None
                    if create_date is None:
                        unresolved_count += 1
                        continue
                    proxy_count += 1

                cur.execute(
                    """
                    UPDATE public.purchase_order
                    SET create_date = %s,
                        user_id = %s,
                        company_id = %s,
                        state = %s,
                        date_approve = %s,
                        name = %s
                    WHERE id = %s
                    """,
                    (create_date, user_id, company_id, state, date_approve, name, po_id),
                )
                if cur.rowcount == 0:
                    cur.execute(
                        """
                        INSERT INTO public.purchase_order
                            (id, create_date, user_id, company_id, state, date_approve, name)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (po_id, create_date, user_id, company_id, state, date_approve, name),
                    )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(
        "[local-relational-fixture] Odoo PO parents restored: "
        f"{exact_count} from silver.dim_po reference, "
        f"{proxy_count} from purchase_order_line create_date proxy, "
        f"{unresolved_count} unresolved"
    )
    print(
        "[local-relational-fixture] PO reference: "
        f"{dim_po_path.relative_to(REPO_ROOT)}"
    )


def restore_editing_overlap() -> None:
    reference = reference_path(
        "LOCAL_FACT_EDITING_REFERENCE",
        DEFAULT_FACT_EDITING_REFERENCE,
    )

    df = pd.read_csv(reference, dtype=str, keep_default_na=False, low_memory=False)
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
    print(f"[local-relational-fixture] reference: {reference.relative_to(REPO_ROOT)}")


def main() -> None:
    restore_odoo_purchase_order_parents()
    restore_editing_overlap()


if __name__ == "__main__":
    main()
