#!/usr/bin/env bash
set -e

# Build a correctly URL-encoded SQLAlchemy URI from raw environment values.
# This supports PostgreSQL passwords containing @, :, / and other URL symbols.
export AIRFLOW_DB_USER="${AIRFLOW_DB_USER:-$DWH_PG_USER}"
export AIRFLOW_DB_PASSWORD="${AIRFLOW_DB_PASSWORD:-$DWH_PG_PASSWORD}"

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="$(
  python - "$AIRFLOW_DB_USER" "$AIRFLOW_DB_PASSWORD" "$AIRFLOW_DB_HOST" \
    "$AIRFLOW_DB_PORT" "$AIRFLOW_DB_NAME" <<'PY'
import sys
from urllib.parse import quote_plus

user, password, host, port, database = sys.argv[1:]
print(
    f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}"
    f"@{host}:{port}/{quote_plus(database)}"
)
PY
)"

exec /entrypoint "$@"
