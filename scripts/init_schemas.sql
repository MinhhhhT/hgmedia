CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;

-- Sổ cái theo dõi mọi lần extract (thay cho _file_registry cũ, dùng chung cho mọi loại nguồn)
CREATE TABLE IF NOT EXISTS meta._source_registry (
    id              BIGSERIAL PRIMARY KEY,
    source_id       VARCHAR(150) NOT NULL,      -- vd: dim_partners, fact_distribution
    source_type     VARCHAR(50)  NOT NULL,      -- excel | google_sheet | sql | elasticsearch
    connection_name VARCHAR(100),                -- vd: odoo_pg, hg_stock, channel_service
    batch_id        VARCHAR(150) NOT NULL UNIQUE,
    minio_path      TEXT NOT NULL,
    checksum        VARCHAR(64),                 -- md5 (excel/gsheet) hoặc null (sql dùng watermark)
    watermark_value VARCHAR(100),                -- giá trị watermark lớn nhất của batch này (nếu có)
    row_count       INTEGER,
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',  -- pending|extracted|loaded|failed|rolled_back
    extracted_at    TIMESTAMP DEFAULT now(),
    loaded_at       TIMESTAMP,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_source_registry_source_id ON meta._source_registry (source_id, extracted_at DESC);

CREATE TABLE IF NOT EXISTS meta.dq_validation_runs (
    validation_run_id UUID PRIMARY KEY,
    airflow_dag_id VARCHAR(250),
    airflow_dag_run_id VARCHAR(250),
    source_id VARCHAR(150) NOT NULL,
    target_table VARCHAR(250) NOT NULL,
    batch_id VARCHAR(150),
    status VARCHAR(30) NOT NULL,
    total_rules INTEGER NOT NULL DEFAULT 0,
    passed_rules INTEGER NOT NULL DEFAULT 0,
    failed_rules INTEGER NOT NULL DEFAULT 0,
    success_percent NUMERIC(5,2),
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    error_message TEXT,
    config_hash VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS meta.dq_validation_results (
    id BIGSERIAL PRIMARY KEY,
    validation_run_id UUID NOT NULL
        REFERENCES meta.dq_validation_runs(validation_run_id),
    source_id VARCHAR(150) NOT NULL,
    rule_id VARCHAR(200) NOT NULL,
    expectation_type VARCHAR(200) NOT NULL,
    column_name VARCHAR(250),
    severity VARCHAR(20) NOT NULL,
    success BOOLEAN NOT NULL,
    observed_value TEXT,
    unexpected_count INTEGER,
    unexpected_percent NUMERIC(10,4),
    result_json JSONB,
    validated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dq_runs_source_time
    ON meta.dq_validation_runs(source_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_dq_results_run
    ON meta.dq_validation_results(validation_run_id);

-- Keep bootstrap self-contained: apply the idempotent dbt DQ schema extension
-- after the base metadata tables exist. \ir resolves relative to this file,
-- so the same script works both in Docker and from a host psql invocation.
\ir 002_dq_dbt.sql
