#!/usr/bin/env bash
set -euo pipefail

truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

if ! truthy "${LOCAL_SOURCE_PROXY_MODE:-false}"; then
  exit 0
fi

if ! command -v socat >/dev/null 2>&1; then
  echo "[fake-source-proxy] socat is required but is not installed" >&2
  exit 1
fi

PG_BACKEND_HOST="${SOURCE_PROXY_PG_BACKEND_HOST:-host.docker.internal}"
PG_BACKEND_PORT="${SOURCE_PROXY_PG_BACKEND_PORT:-5433}"
MSSQL_BACKEND_HOST="${SOURCE_PROXY_MSSQL_BACKEND_HOST:-host.docker.internal}"
MSSQL_BACKEND_PORT="${SOURCE_PROXY_MSSQL_BACKEND_PORT:-1434}"

start_proxy() {
  local label="$1"
  local bind_ip="$2"
  local bind_port="$3"
  local target_host="$4"
  local target_port="$5"

  socat \
    "TCP4-LISTEN:${bind_port},bind=${bind_ip},reuseaddr,fork" \
    "TCP4:${target_host}:${target_port}" \
    >/tmp/fake-source-proxy-${label}.log 2>&1 &
  local pid=$!

  sleep 0.05
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "[fake-source-proxy] failed to start ${label} on ${bind_ip}:${bind_port}" >&2
    cat "/tmp/fake-source-proxy-${label}.log" >&2 || true
    exit 1
  fi

  echo "[fake-source-proxy] ${label}: ${bind_ip}:${bind_port} -> ${target_host}:${target_port}"
}

# PostgreSQL source systems. They intentionally have different fake IPs even
# though the lightweight local lab uses one PostgreSQL backend container.
start_proxy odoo              127.20.0.11 5432 "$PG_BACKEND_HOST" "$PG_BACKEND_PORT"
start_proxy hg_stock          127.20.0.12 5432 "$PG_BACKEND_HOST" "$PG_BACKEND_PORT"

# SQL Server source systems. Each logical production system gets its own fake
# endpoint while sharing one local SQL Server backend that hosts separate DBs.
start_proxy editing           127.20.0.21 1433 "$MSSQL_BACKEND_HOST" "$MSSQL_BACKEND_PORT"
start_proxy record_survey     127.20.0.22 1433 "$MSSQL_BACKEND_HOST" "$MSSQL_BACKEND_PORT"
start_proxy channel_accountant 127.20.0.30 1433 "$MSSQL_BACKEND_HOST" "$MSSQL_BACKEND_PORT"
start_proxy channel_channel   127.20.0.31 1433 "$MSSQL_BACKEND_HOST" "$MSSQL_BACKEND_PORT"
start_proxy channel_network   127.20.0.32 1433 "$MSSQL_BACKEND_HOST" "$MSSQL_BACKEND_PORT"
start_proxy channel_org       127.20.0.33 1433 "$MSSQL_BACKEND_HOST" "$MSSQL_BACKEND_PORT"
start_proxy channel_project   127.20.0.34 1433 "$MSSQL_BACKEND_HOST" "$MSSQL_BACKEND_PORT"
start_proxy channel_relation  127.20.0.35 1433 "$MSSQL_BACKEND_HOST" "$MSSQL_BACKEND_PORT"
