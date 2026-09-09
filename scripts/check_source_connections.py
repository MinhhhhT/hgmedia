#!/usr/bin/env python3
"""Preflight connectivity checks for configured live SQL source systems.

The script reads the same .env and src.connections definitions used by the EL
pipeline. It never prints passwords. For each SQL connection referenced by
config/db_sources.yaml it performs:
1) TCP reachability to host:port;
2) database login and SELECT 1.

Usage:
  python scripts/check_source_connections.py
  python scripts/check_source_connections.py --connection odoo_pg
"""
from __future__ import annotations

import argparse
import socket
import sys
from pathlib import Path

import psycopg2
import pyodbc
import yaml
from dotenv import load_dotenv

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

load_dotenv(REPO / ".env", override=True)

from src.connections import get_connection  # noqa: E402


def configured_sql_connections() -> list[str]:
    data = yaml.safe_load((REPO / "config" / "db_sources.yaml").read_text(encoding="utf-8")) or {}
    names = {
        item["connection"]
        for item in data.get("db_sources", [])
        if item.get("source_type") == "sql" and item.get("connection")
    }
    return sorted(names)


def tcp_check(host: str, port: int, timeout: float = 5.0) -> None:
    with socket.create_connection((host, port), timeout=timeout):
        return


def query_check(conn: dict, timeout: int = 8) -> None:
    if conn["type"] == "postgresql":
        db = psycopg2.connect(
            host=conn["host"],
            port=conn["port"],
            dbname=conn["database"],
            user=conn["user"],
            password=conn["password"],
            connect_timeout=timeout,
        )
        try:
            with db.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        finally:
            db.close()
        return

    if conn["type"] == "sqlserver":
        cs = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={conn['host']},{conn['port']};"
            f"DATABASE={conn['database']};"
            f"UID={conn['user']};PWD={conn['password']};"
            "TrustServerCertificate=yes;Encrypt=no;"
            f"Connection Timeout={timeout};"
        )
        db = pyodbc.connect(cs)
        try:
            cur = db.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
        finally:
            db.close()
        return

    raise ValueError(f"Unsupported SQL connection type: {conn['type']}")


def check_one(name: str) -> bool:
    try:
        conn = get_connection(name)
    except Exception as exc:
        print(f"[FAIL] {name}: configuration error: {exc}")
        return False

    target = f"{conn['host']}:{conn['port']}/{conn['database']}"
    try:
        tcp_check(conn["host"], int(conn["port"]))
    except Exception as exc:
        print(f"[FAIL] {name:<24} {target} | TCP: {type(exc).__name__}: {exc}")
        return False

    try:
        query_check(conn)
    except Exception as exc:
        print(f"[FAIL] {name:<24} {target} | login/query: {type(exc).__name__}: {exc}")
        return False

    print(f"[PASS] {name:<24} {target}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection", help="Check one logical connection name")
    args = parser.parse_args()

    names = [args.connection] if args.connection else configured_sql_connections()
    passed = sum(1 for name in names if check_one(name))
    failed = len(names) - passed

    print(f"\nConnection preflight: {passed} passed, {failed} failed, {len(names)} total")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
