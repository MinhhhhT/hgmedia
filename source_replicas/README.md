# HG Media local source replicas

This directory reconstructs the **database sources that are actually used by
`config/db_sources.yaml`**. It is intended for offline development, pipeline
recovery and repeatable end-to-end tests when the historical production
connections are no longer available.

## What is generated

`source_replicas/generated/` contains DDL for exactly **54 database source
tables**:

- Odoo PostgreSQL: 18 tables (`public`)
- HG Stock PostgreSQL: 11 tables (`dbo` schema)
- Editing SQL Server: 4 tables
- Record Survey SQL Server: 6 tables
- Channel Service SQL Server: 15 tables across five databases

The table list comes from `config/db_sources.yaml`.

Column metadata is resolved in this order:

1. `HG_Media_Source_Data_Dictionary_v2.xlsm` for documented table/field types.
2. The latest staging sample headers in
   `exports/hgmediadb_samples_1000_20260828_000045/` to reproduce the physical
   columns that the current pipeline actually receives.
3. Sample-value inference only when the Source Data Dictionary does not document
   a physical/inherited/audit field.

The exact provenance of every generated column is recorded by the generator in
`source_replicas/reports/source_dictionary_coverage.csv`.

## Fake source network

Local SQL sources are deliberately exposed through **fake, unroutable loopback
IP/port pairs** inside each Airflow/bootstrap runtime.  The EL code therefore
behaves as if it were connecting to multiple independent external systems while
all traffic is forwarded only to disposable local Docker replicas.

| Logical source | Fake endpoint |
| --- | --- |
| Odoo PostgreSQL | `127.20.0.11:5432` |
| HG Stock PostgreSQL | `127.20.0.12:5432` |
| Editing SQL Server | `127.20.0.21:1433` |
| Record Survey SQL Server | `127.20.0.22:1433` |
| Channel Accountant | `127.20.0.30:1433` |
| Channel | `127.20.0.31:1433` |
| Network | `127.20.0.32:1433` |
| Organization | `127.20.0.33:1433` |
| Project | `127.20.0.34:1433` |
| Relationship | `127.20.0.35:1433` |

These addresses belong to `127.0.0.0/8`; they cannot route to HG production
networks. `scripts/start_fake_source_proxies.sh` creates local `socat` listeners
and forwards them to the sample-data replicas. `host.docker.internal` is used
only behind that proxy layer for GitHub Codespaces compatibility and is never a
logical source endpoint seen by the EL connection configuration.

The lightweight lab intentionally uses one PostgreSQL backend for Odoo/HG Stock
and one SQL Server backend for all SQL Server databases.  The fake endpoint layer
still preserves distinct source-system IP/port identities without requiring many
resource-heavy SQL Server containers.

## Regenerate after a dictionary change

The source dictionary is deliberately not committed into this repository.
Regenerate DDL with:

```bash
python scripts/generate_source_ddl.py \
  --dictionary "/secure/path/HG_Media_Source_Data_Dictionary_v2.xlsm"
```

## Local reconstruction

```bash
cp .env.rebuild.example .env

docker compose \
  -f docker-compose.yml \
  -f docker-compose.rebuild.yml \
  up -d dwh-postgres source-postgres source-mssql minio

docker compose \
  -f docker-compose.yml \
  -f docker-compose.rebuild.yml \
  run --rm source-bootstrap
```

`source-bootstrap` creates all source databases/tables and seeds the database
sources from the sample staging exports already present in the repository.
Pipeline metadata columns are removed before seeding because they are not
physical source fields.  Bootstrap itself also connects through the fake source
endpoints, so the same routing model is exercised before Airflow starts.

With `LOCAL_FIXTURE_MODE=true`, Google Sheet, Elasticsearch, Sale API, CSV and FX
sources currently replay exported fixtures. They do not contact live systems.
Those 12 non-SQL sources are fixture-isolated rather than network-mocked at this
stage.

Set `LOCAL_FIXTURE_MODE=false` only when intentionally switching to live
non-database connectors and after supplying explicit live credentials/endpoints.

## Security

`.env` is ignored by Git. Only `.env.example` and `.env.rebuild.example` may be
versioned. Do not commit historical credentials, Google service-account JSON,
private API endpoints or real production passwords.
