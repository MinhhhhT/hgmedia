#!/usr/bin/env python3
"""Create and optionally seed local database source replicas.

DDL is generated from the HG Source Data Dictionary plus the latest staging
sample export. The seed step replays those staging samples back into physical
source tables after removing pipeline metadata columns.

Run inside the project Docker network:
  python scripts/bootstrap_source_replicas.py --seed
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import psycopg2
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DDL_ROOT = REPO_ROOT / "source_replicas" / "generated"


def env(name: str, default: str = "", required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def wait_for_postgres(host: str, port: int, user: str, password: str, timeout: int = 180):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            conn = psycopg2.connect(
                host=host, port=port, dbname="postgres", user=user,
                password=password, connect_timeout=5,
            )
            conn.close()
            return
        except Exception as exc:
            last = exc
            time.sleep(2)
    raise RuntimeError(f"PostgreSQL source replica did not become ready: {last}")


def wait_for_mssql(host: str, port: int, user: str, password: str, timeout: int = 240):
    deadline = time.time() + timeout
    last = None
    cs = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={host},{port};DATABASE=master;UID={user};PWD={password};"
        "TrustServerCertificate=yes;Encrypt=no;Connection Timeout=5;"
    )
    while time.time() < deadline:
        try:
            conn = pyodbc.connect(cs, autocommit=True)
            conn.close()
            return
        except Exception as exc:
            last = exc
            time.sleep(3)
    raise RuntimeError(f"SQL Server source replica did not become ready: {last}")


def ensure_postgres_database(host: str, port: int, user: str, password: str, database: str):
    conn = psycopg2.connect(host=host, port=port, dbname="postgres", user=user, password=password)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE "{database.replace(chr(34), chr(34)*2)}"')
                print(f"[postgres] created database {database}")
    finally:
        conn.close()


def apply_postgres_ddl(host: str, port: int, user: str, password: str, database: str, ddl_path: Path):
    conn = psycopg2.connect(host=host, port=port, dbname=database, user=user, password=password)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(ddl_path.read_text(encoding="utf-8"))
        print(f"[postgres] applied {ddl_path.name} -> {database}")
    finally:
        conn.close()


def split_go_batches(sql: str):
    return [x.strip() for x in re.split(r"^\s*GO\s*$", sql, flags=re.I | re.M) if x.strip()]


def apply_mssql_ddl(host: str, port: int, user: str, password: str, ddl_path: Path):
    cs = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={host},{port};DATABASE=master;UID={user};PWD={password};"
        "TrustServerCertificate=yes;Encrypt=no;Connection Timeout=30;"
    )
    conn = pyodbc.connect(cs, autocommit=True)
    try:
        cur = conn.cursor()
        for batch in split_go_batches(ddl_path.read_text(encoding="utf-8")):
            cur.execute(batch)
            while cur.nextset():
                pass
        print(f"[mssql] applied {ddl_path.name}")
    finally:
        conn.close()


def pg_uri(host: str, port: int, database: str, user: str, password: str) -> str:
    return (
        f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{quote_plus(database)}"
    )


def mssql_uri(host: str, port: int, database: str, user: str, password: str) -> str:
    return (
        f"mssql+pyodbc://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{quote_plus(database)}"
        "?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&Encrypt=no"
    )


def convert_value(value, sql_type: str):
    if value is None:
        return None
    raw = str(value).strip()
    if not raw or raw.lower() in {"nan", "nat", "none", "null"}:
        return None
    typ = sql_type.upper()
    if "BOOLEAN" in typ or typ == "BIT":
        low = raw.lower()
        if low in {"1", "true", "t", "yes", "y"}:
            return True
        if low in {"0", "false", "f", "no", "n"}:
            return False
        return None
    if "BIGINT" in typ or re.fullmatch(r"INT(?:EGER)?", typ):
        try:
            return int(Decimal(raw))
        except (InvalidOperation, ValueError):
            return None
    if any(token in typ for token in ("NUMERIC", "DECIMAL")):
        try:
            return Decimal(raw)
        except InvalidOperation:
            return None
    if "DOUBLE" in typ or typ == "FLOAT" or typ.startswith("REAL"):
        try:
            return float(raw)
        except ValueError:
            return None
    if any(token in typ for token in ("TIMESTAMP", "DATETIME")):
        try:
            candidate = raw.replace("Z", "+00:00")
            dt = datetime.fromisoformat(candidate)
            return dt.replace(tzinfo=None)
        except ValueError:
            parsed = pd.to_datetime(raw, errors="coerce", utc=True)
            if pd.isna(parsed):
                return None
            return parsed.to_pydatetime().replace(tzinfo=None)
    if typ == "DATE":
        try:
            return datetime.fromisoformat(raw[:10]).date()
        except ValueError:
            parsed = pd.to_datetime(raw, errors="coerce")
            return None if pd.isna(parsed) else parsed.date()
    if "BYTEA" in typ or "VARBINARY" in typ:
        return raw.encode("utf-8")
    return raw


def seed_one(engine, item: dict):
    fixture = REPO_ROOT / item["fixture_csv"]
    df = pd.read_csv(fixture, dtype=str, keep_default_na=False, low_memory=False)
    columns = item["source_columns"]
    df = df[[c for c in columns if c in df.columns]].copy()
    for col in columns:
        if col not in df.columns:
            df[col] = None
    df = df[columns]

    for col, typ in item["column_types"].items():
        df[col] = [convert_value(v, typ) for v in df[col].tolist()]

    schema, table = item["schema"], item["table"]
    if item["dialect"] == "postgresql":
        qtable = f'"{schema}"."{table}"'
    else:
        qtable = f"[{schema}].[{table}]"
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {qtable}"))
    df.to_sql(
        table,
        engine,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=200,
        method=None,
    )
    print(f"[seed] {item['source_id']}: {len(df)} rows -> {item['database']}.{schema}.{table}")


def main():
    load_dotenv(REPO_ROOT / ".env")
    ap = argparse.ArgumentParser()
    ap.add_argument("--ddl-root", type=Path, default=DEFAULT_DDL_ROOT)
    ap.add_argument("--seed", action="store_true", help="Seed from exported staging fixture CSV files")
    args = ap.parse_args()

    pg_host = env("SOURCE_PG_ADMIN_HOST", env("ODOO_PG_HOST", "source-postgres"))
    pg_port = int(env("SOURCE_PG_ADMIN_PORT", env("ODOO_PG_PORT", "5432")))
    pg_user = env("SOURCE_PG_ADMIN_USER", env("ODOO_PG_USER"), required=True)
    pg_password = env("SOURCE_PG_ADMIN_PASSWORD", env("ODOO_PG_PASSWORD"), required=True)

    ms_host = env("SOURCE_MSSQL_ADMIN_HOST", env("EDITING_HOST", "source-mssql"))
    ms_port = int(env("SOURCE_MSSQL_ADMIN_PORT", env("EDITING_PORT", "1433")))
    ms_user = env("SOURCE_MSSQL_ADMIN_USER", "sa")
    ms_password = env("SOURCE_MSSQL_SA_PASSWORD", env("EDITING_PASSWORD"), required=True)

    wait_for_postgres(pg_host, pg_port, pg_user, pg_password)
    wait_for_mssql(ms_host, ms_port, ms_user, ms_password)

    for database, ddl_file in (
        (env("ODOO_PG_DB", "odoo"), args.ddl_root / "postgres" / "odoo_pg.sql"),
        (env("HGSTOCK_DB", "hgstock"), args.ddl_root / "postgres" / "hg_stock.sql"),
    ):
        ensure_postgres_database(pg_host, pg_port, pg_user, pg_password, database)
        apply_postgres_ddl(pg_host, pg_port, pg_user, pg_password, database, ddl_file)

    for ddl_file in sorted((args.ddl_root / "sqlserver").glob("*.sql")):
        apply_mssql_ddl(ms_host, ms_port, ms_user, ms_password, ddl_file)

    if not args.seed:
        return

    manifest = json.loads((args.ddl_root / "manifest.json").read_text(encoding="utf-8"))
    engines = {}
    try:
        for item in manifest:
            key = (item["dialect"], item["database"])
            if key not in engines:
                if item["dialect"] == "postgresql":
                    engines[key] = create_engine(
                        pg_uri(pg_host, pg_port, item["database"], pg_user, pg_password),
                        pool_pre_ping=True,
                    )
                else:
                    engines[key] = create_engine(
                        mssql_uri(ms_host, ms_port, item["database"], ms_user, ms_password),
                        pool_pre_ping=True,
                        fast_executemany=True,
                    )
            seed_one(engines[key], item)
    finally:
        for engine in engines.values():
            engine.dispose()

    print(f"Source replica bootstrap complete: {len(manifest)} tables seeded")


if __name__ == "__main__":
    main()
