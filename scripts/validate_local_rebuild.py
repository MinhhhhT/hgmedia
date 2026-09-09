#!/usr/bin/env python3
"""Static validation for the offline HG source-rebuild assets."""
from __future__ import annotations

import ipaddress
import json
import os
import sys
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

EXPECTED_SQL_CONNECTIONS = {
    "odoo_pg",
    "hg_stock",
    "editing_management",
    "record_survey",
    "channel_channel",
    "channel_network",
    "channel_organization",
    "channel_project",
    "channel_relationship",
}


def main() -> int:
    load_dotenv(REPO / ".env", override=True)
    os.environ.setdefault("LOCAL_FIXTURE_MODE", "true")

    from src.config_loader import load_all_sources
    from src.connections import get_connection

    sources = load_all_sources(str(REPO / "config"))
    db_sources = [s for s in sources if s["source_type"] == "sql"]
    non_db = [s for s in sources if s["source_type"] != "sql"]
    manifest = json.loads(
        (REPO / "source_replicas" / "generated" / "manifest.json").read_text(encoding="utf-8")
    )

    errors = []
    if len(sources) != 66:
        errors.append(f"Expected 66 sources, found {len(sources)}")
    if len(db_sources) != 54:
        errors.append(f"Expected 54 SQL sources, found {len(db_sources)}")
    if len(manifest) != len(db_sources):
        errors.append(f"DDL manifest has {len(manifest)} tables, SQL config has {len(db_sources)}")

    manifest_ids = {x["source_id"] for x in manifest}
    config_ids = {x["source_id"] for x in db_sources}
    if manifest_ids != config_ids:
        errors.append(
            f"Manifest/config source mismatch: missing={sorted(config_ids-manifest_ids)}, "
            f"extra={sorted(manifest_ids-config_ids)}"
        )

    configured_connections = {s.get("connection") for s in db_sources}
    if configured_connections != EXPECTED_SQL_CONNECTIONS:
        errors.append(
            "Expected exactly nine active SQL connection groups: "
            f"missing={sorted(EXPECTED_SQL_CONNECTIONS-configured_connections)}, "
            f"extra={sorted(configured_connections-EXPECTED_SQL_CONNECTIONS)}"
        )

    # Local rebuild must expose each logical SQL system through a distinct,
    # unroutable loopback endpoint. This prevents accidental use of production
    # IPs and prevents multiple logical systems from silently collapsing onto a
    # single local connection.
    if os.environ.get("LOCAL_SOURCE_PROXY_MODE", "").strip().lower() in {"1", "true", "yes", "on"}:
        endpoints = {}
        for name in sorted(EXPECTED_SQL_CONNECTIONS):
            try:
                conn = get_connection(name)
            except Exception as exc:
                errors.append(f"Fake source connection {name} is invalid: {exc}")
                continue
            host = str(conn.get("host", ""))
            port = int(conn.get("port", 0))
            try:
                ip = ipaddress.ip_address(host)
            except ValueError:
                errors.append(f"Fake source {name} host must be an IP address, got {host!r}")
                continue
            if not (ip.version == 4 and host.startswith("127.20.0.")):
                errors.append(f"Fake source {name} must use 127.20.0.x, got {host}:{port}")
            endpoints[name] = (host, port)

        duplicate_endpoints = [
            endpoint for endpoint, count in Counter(endpoints.values()).items() if count > 1
        ]
        if duplicate_endpoints:
            errors.append(f"Fake SQL source endpoints are not unique: {duplicate_endpoints}")
        if len(endpoints) != 9:
            errors.append(f"Expected 9 validated fake SQL endpoints, found {len(endpoints)}")

    for item in manifest:
        fixture = REPO / item["fixture_csv"]
        if not fixture.exists():
            errors.append(f"Missing DB fixture: {fixture}")
        if not item.get("source_columns"):
            errors.append(f"No physical columns generated for {item['source_id']}")

    for src in non_db:
        fixture_value = src.get("fixture_csv")
        if not fixture_value:
            errors.append(f"Non-SQL source lacks local fixture: {src['source_id']}")
            continue
        fixture = REPO / fixture_value
        if not fixture.exists():
            errors.append(f"Missing non-SQL fixture for {src['source_id']}: {fixture}")
            continue
        with fixture.open("rb") as handle:
            line_count = sum(1 for _ in handle)
        if line_count < 2:
            errors.append(f"Fixture has header only: {src['source_id']} -> {fixture}")

    forbidden = [
        "192.168.8.125:3131",
        "PLACEHOLDER_HOST",
        "PLACEHOLDER_USER",
        "PLACEHOLDER_PASS",
        "hgmedia@123",
    ]
    scan_files = [
        REPO / "src" / "connections.py",
        REPO / "config" / "api_sources.yaml",
        REPO / "dwh_dbt" / "profiles.yml",
    ]
    for path in scan_files:
        text = path.read_text(encoding="utf-8")
        for token in forbidden:
            if token in text:
                errors.append(f"Forbidden hard-coded connection token in {path}: {token}")

    if errors:
        print("LOCAL REBUILD VALIDATION: FAILED")
        for error in errors:
            print(f" - {error}")
        return 1

    print("LOCAL REBUILD VALIDATION: OK")
    print(f" - all sources: {len(sources)}")
    print(f" - SQL source replicas: {len(db_sources)}")
    print(f" - active SQL source systems: {len(EXPECTED_SQL_CONNECTIONS)}")
    print(f" - non-SQL local fixtures: {len(non_db)}")
    print(f" - generated physical columns: {sum(len(x['source_columns']) for x in manifest)}")
    if os.environ.get("LOCAL_SOURCE_PROXY_MODE", "").strip().lower() in {"1", "true", "yes", "on"}:
        print(" - fake SQL endpoint topology: OK (9 unique 127.20.0.x IP/port pairs)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
