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
physical source fields.

With `LOCAL_FIXTURE_MODE=true`, Google Sheet, Elasticsearch, Sale API, CSV and FX
sources replay exported fixtures while all SQL sources are still read from the
reconstructed PostgreSQL/SQL Server databases through `SQLExtractor`.

Set `LOCAL_FIXTURE_MODE=false` and provide real values in `.env` to switch back
to live non-database connectors.

## Security

`.env` is ignored by Git. Only `.env.example` and `.env.rebuild.example` may be
versioned. Do not commit historical credentials, Google service-account JSON,
private API endpoints or real production passwords.
