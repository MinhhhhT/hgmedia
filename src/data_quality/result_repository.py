import json
from datetime import datetime

from sqlalchemy import create_engine, text

from src.connections import get_connection, get_sqlalchemy_uri


class DataQualityRepository:
    def __init__(self):
        conn_cfg = get_connection("dwh_postgres")
        self.engine = create_engine(get_sqlalchemy_uri(conn_cfg))

    def create_run(
        self,
        validation_run_id: str,
        airflow_dag_id: str,
        airflow_dag_run_id: str,
        source_id: str,
        target_table: str,
        batch_id: str | None,
        status: str,
        config_hash: str | None,
        dbt_model: str | None = None,
        dbt_layer: str | None = None,
        parent_dag_id: str | None = None,
        parent_dag_run_id: str | None = None,
        records_checked: int | None = None,
    ) -> None:
        query = text("""
            INSERT INTO meta.dq_validation_runs (
                validation_run_id,
                airflow_dag_id,
                airflow_dag_run_id,
                source_id,
                target_table,
                batch_id,
                status,
                started_at,
                config_hash,
                dbt_model,
                dbt_layer,
                parent_dag_id,
                parent_dag_run_id,
                records_checked
            )
            VALUES (
                CAST(:validation_run_id AS uuid),
                :airflow_dag_id,
                :airflow_dag_run_id,
                :source_id,
                :target_table,
                :batch_id,
                :status,
                :started_at,
                :config_hash,
                :dbt_model,
                :dbt_layer,
                :parent_dag_id,
                :parent_dag_run_id,
                :records_checked
            )
        """)

        with self.engine.begin() as conn:
            conn.execute(query, {
                "validation_run_id": validation_run_id,
                "airflow_dag_id": airflow_dag_id,
                "airflow_dag_run_id": airflow_dag_run_id,
                "source_id": source_id,
                "target_table": target_table,
                "batch_id": batch_id,
                "status": status,
                "started_at": datetime.now(),
                "config_hash": config_hash,
                "dbt_model": dbt_model,
                "dbt_layer": dbt_layer,
                "parent_dag_id": parent_dag_id,
                "parent_dag_run_id": parent_dag_run_id,
                "records_checked": records_checked,
            })

    def save_rule_results(
        self,
        validation_run_id: str,
        source_id: str,
        results: list[dict],
    ) -> None:
        if not results:
            return

        query = text("""
            INSERT INTO meta.dq_validation_results (
                validation_run_id,
                source_id,
                rule_id,
                expectation_type,
                column_name,
                severity,
                success,
                observed_value,
                unexpected_count,
                unexpected_percent,
                result_json
            )
            VALUES (
                CAST(:validation_run_id AS uuid),
                :source_id,
                :rule_id,
                :expectation_type,
                :column_name,
                :severity,
                :success,
                :observed_value,
                :unexpected_count,
                :unexpected_percent,
                CAST(:result_json AS jsonb)
            )
        """)

        payload = []
        for result in results:
            payload.append({
                "validation_run_id": validation_run_id,
                "source_id": source_id,
                "rule_id": result["rule_id"],
                "expectation_type": result["expectation_type"],
                "column_name": result.get("column_name"),
                "severity": result["severity"],
                "success": result["success"],
                "observed_value": result.get("observed_value"),
                "unexpected_count": result.get("unexpected_count"),
                "unexpected_percent": result.get("unexpected_percent"),
                "result_json": json.dumps(
                    result.get("raw_result", {}),
                    ensure_ascii=False,
                    default=str,
                ),
            })

        with self.engine.begin() as conn:
            conn.execute(query, payload)

    def finish_run(
        self,
        validation_run_id: str,
        status: str,
        total_rules: int,
        passed_rules: int,
        failed_rules: int,
        error_message: str | None = None,
        records_checked: int | None = None,
        duration_seconds: float | None = None,
    ) -> None:
        success_percent = (
            round((passed_rules / total_rules) * 100, 2)
            if total_rules
            else 0
        )

        query = text("""
            UPDATE meta.dq_validation_runs
            SET
                status = :status,
                total_rules = :total_rules,
                passed_rules = :passed_rules,
                failed_rules = :failed_rules,
                success_percent = :success_percent,
                finished_at = :finished_at,
                error_message = :error_message,
                records_checked = COALESCE(
                    :records_checked,
                    records_checked
                ),
                duration_seconds = COALESCE(
                    :duration_seconds,
                    duration_seconds
                )
            WHERE validation_run_id = CAST(:validation_run_id AS uuid)
        """)

        with self.engine.begin() as conn:
            conn.execute(query, {
                "validation_run_id": validation_run_id,
                "status": status,
                "total_rules": total_rules,
                "passed_rules": passed_rules,
                "failed_rules": failed_rules,
                "success_percent": success_percent,
                "finished_at": datetime.now(),
                "error_message": error_message,
                "records_checked": records_checked,
                "duration_seconds": duration_seconds,
            })

    def save_unexpected_samples(
        self,
        validation_run_id: str,
        rule_id: str,
        rows: list[dict],
        limit: int = 20,
    ) -> None:
        if not rows:
            return

        query = text("""
            INSERT INTO meta.dq_unexpected_samples (
                validation_run_id,
                rule_id,
                sample_no,
                row_data
            )
            VALUES (
                CAST(:validation_run_id AS uuid),
                :rule_id,
                :sample_no,
                CAST(:row_data AS jsonb)
            )
        """)

        payload = [
            {
                "validation_run_id": validation_run_id,
                "rule_id": rule_id,
                "sample_no": index,
                "row_data": json.dumps(row, ensure_ascii=False, default=str),
            }
            for index, row in enumerate(rows[:limit], start=1)
        ]

        with self.engine.begin() as conn:
            conn.execute(query, payload)
