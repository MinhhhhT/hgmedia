"""Central connection definitions.

No production endpoint or credential is versioned here. Deployment-specific
values come from .env, Airflow secrets/connections, or a secret manager.

The runtime supports two distinct modes:
- live sources: every source system can have its own host/port/credentials;
- local rebuild: .env.rebuild.example points those same logical connections at
  disposable local replicas for offline testing.
"""
from __future__ import annotations

import os
from urllib.parse import quote_plus

from dotenv import load_dotenv

load_dotenv()


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _first_env(*keys: str, default: str = "") -> str:
    """Return the first non-empty environment value from keys."""
    for key in keys:
        value = os.environ.get(key)
        if value is not None and str(value).strip() != "":
            return str(value)
    return default


CONNECTIONS = {
    "record_survey": {
        "type": "sqlserver",
        "host": _env("RS_HOST"),
        "port": int(_env("RS_PORT", "1433")),
        "database": _env("RS_DB", "record-survey"),
        "user": _env("RS_USER"),
        "password": _env("RS_PASSWORD"),
        "default_schema": "dbo",
    },
    "elastic": {
        "type": "elastic",
        "host": _env("ELASTIC_HOST"),
        "user": _env("ELASTIC_USER"),
        "password": _env("ELASTIC_PASSWORD"),
    },
    "minio": {
        "endpoint": _env("MINIO_ENDPOINT", "localhost:9000"),
        "access_key": _env("MINIO_ACCESS_KEY"),
        "secret_key": _env("MINIO_SECRET_KEY"),
        "secure": _env("MINIO_SECURE", "false").lower() in {"1", "true", "yes"},
        "bucket_raw": _env("MINIO_RAW_BUCKET", "raw-bronze"),
    },
    "dwh_postgres": {
        "type": "postgresql",
        "host": _env("DWH_PG_HOST", "localhost"),
        "port": int(_env("DWH_PG_PORT", "5432")),
        "database": _env("DWH_PG_DB", "data_warehouse"),
        "user": _env("DWH_PG_USER"),
        "password": _env("DWH_PG_PASSWORD"),
    },
    "odoo_pg": {
        "type": "postgresql",
        "host": _env("ODOO_PG_HOST"),
        "port": int(_env("ODOO_PG_PORT", "5432")),
        "database": _env("ODOO_PG_DB", "odoo"),
        "user": _env("ODOO_PG_USER"),
        "password": _env("ODOO_PG_PASSWORD"),
        "default_schema": "public",
    },
    "hg_stock": {
        "type": "postgresql",
        "host": _env("HGSTOCK_HOST"),
        "port": int(_env("HGSTOCK_PORT", "5432")),
        "database": _env("HGSTOCK_DB", "hgstock"),
        "user": _env("HGSTOCK_USER"),
        "password": _env("HGSTOCK_PASSWORD"),
        "default_schema": "dbo",
    },
    "editing_management": {
        "type": "sqlserver",
        "host": _env("EDITING_HOST"),
        "port": int(_env("EDITING_PORT", "1433")),
        "database": _env("EDITING_DB", "editing-management"),
        "user": _env("EDITING_USER"),
        "password": _env("EDITING_PASSWORD"),
        "default_schema": "dbo",
    },
    "google_sheet": {
        "type": "google_sheet",
        "credentials_json_path": _env("GSHEET_CREDENTIALS_PATH", "config/gsheet_service_account.json"),
    },
}

# Each Channel service can live on a different SQL Server endpoint.  The legacy
# CHANNEL_HOST/PORT/USER/PASSWORD variables remain as optional shared fallbacks
# for deployments where all Channel databases are hosted together.
_CHANNEL_CONNECTIONS = {
    "channel_accountant": ("CHANNEL_ACCOUNTANT", "channel.service.accountant"),
    "channel_channel": ("CHANNEL_CHANNEL", "channel.service.channel"),
    "channel_network": ("CHANNEL_NETWORK", "channel.service.network"),
    "channel_organization": ("CHANNEL_ORGANIZATION", "channel.service.organization"),
    "channel_project": ("CHANNEL_PROJECT", "channel.service.project"),
    "channel_relationship": ("CHANNEL_RELATIONSHIP", "channel.service.relationship"),
}

for _name, (_prefix, _default_db) in _CHANNEL_CONNECTIONS.items():
    CONNECTIONS[_name] = {
        "type": "sqlserver",
        "host": _first_env(f"{_prefix}_HOST", "CHANNEL_HOST"),
        "port": int(_first_env(f"{_prefix}_PORT", "CHANNEL_PORT", default="1433")),
        "database": _first_env(f"{_prefix}_DB", default=_default_db),
        "user": _first_env(f"{_prefix}_USER", "CHANNEL_USER"),
        "password": _first_env(f"{_prefix}_PASSWORD", "CHANNEL_PASSWORD"),
        "default_schema": "dbo",
    }


def get_connection(name: str) -> dict:
    if name not in CONNECTIONS:
        raise ValueError(f"Connection '{name}' chưa được khai báo trong connections.py")
    conn = CONNECTIONS[name]
    if conn.get("type") in {"postgresql", "sqlserver"}:
        missing = [k for k in ("host", "database", "user", "password") if not conn.get(k)]
        if missing:
            raise ValueError(
                f"Connection '{name}' thiếu {', '.join(missing)}. "
                "Hãy cấu hình các biến tương ứng trong .env/Secret Manager."
            )
    if conn.get("type") == "elastic" and not conn.get("host"):
        raise ValueError("Connection 'elastic' thiếu ELASTIC_HOST")
    return conn


def get_sqlalchemy_uri(conn: dict) -> str:
    user = quote_plus(str(conn["user"]))
    pwd = quote_plus(str(conn["password"]))
    if conn["type"] == "postgresql":
        return f"postgresql+psycopg2://{user}:{pwd}@{conn['host']}:{conn['port']}/{quote_plus(str(conn['database']))}"
    if conn["type"] == "sqlserver":
        return (
            f"mssql+pyodbc://{user}:{pwd}@{conn['host']}:{conn['port']}/{quote_plus(str(conn['database']))}"
            "?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
        )
    raise ValueError(f"Không hỗ trợ build URI cho type: {conn['type']}")
