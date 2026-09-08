CREATE SCHEMA IF NOT EXISTS reporting;

ALTER TABLE meta.dq_validation_runs
    ADD COLUMN IF NOT EXISTS dbt_model VARCHAR(250),
    ADD COLUMN IF NOT EXISTS dbt_layer VARCHAR(30),
    ADD COLUMN IF NOT EXISTS parent_dag_id VARCHAR(250),
    ADD COLUMN IF NOT EXISTS parent_dag_run_id VARCHAR(250),
    ADD COLUMN IF NOT EXISTS records_checked BIGINT,
    ADD COLUMN IF NOT EXISTS duration_seconds NUMERIC(12,2);

CREATE TABLE IF NOT EXISTS meta.dq_unexpected_samples (
    id BIGSERIAL PRIMARY KEY,
    validation_run_id UUID NOT NULL
        REFERENCES meta.dq_validation_runs(validation_run_id),
    rule_id VARCHAR(200) NOT NULL,
    sample_no SMALLINT NOT NULL,
    row_data JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dq_samples_run_rule
    ON meta.dq_unexpected_samples(validation_run_id, rule_id);

CREATE OR REPLACE VIEW reporting.vw_dq_rule_results AS
SELECT
    r.validation_run_id,
    r.started_at::date AS validation_date,
    r.started_at,
    r.finished_at,
    r.airflow_dag_id,
    r.airflow_dag_run_id,
    r.parent_dag_id,
    r.parent_dag_run_id,
    r.dbt_layer,
    r.dbt_model,
    r.target_table,
    r.status AS run_status,
    r.total_rules,
    r.passed_rules,
    r.failed_rules,
    r.success_percent,
    d.rule_id,
    d.expectation_type,
    d.column_name,
    d.severity,
    d.success,
    d.observed_value,
    d.unexpected_count,
    d.unexpected_percent,
    d.validated_at
FROM meta.dq_validation_runs r
JOIN meta.dq_validation_results d
  ON d.validation_run_id = r.validation_run_id;