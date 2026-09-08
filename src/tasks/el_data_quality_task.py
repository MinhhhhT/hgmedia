import hashlib
import json
import logging
import time
from uuid import uuid4

from airflow.exceptions import AirflowException

from src.data_quality.gx_validator import (
    load_staging_dataframe,
    validate_dataframe,
)
from src.data_quality.result_repository import DataQualityRepository
from src.data_quality.staging_rule_loader import (
    load_staging_rule_config,
    resolve_staging_targets,
)


logger = logging.getLogger(__name__)


def _config_hash(table_config: dict) -> str:
    """Tạo hash xác định phiên bản rule config."""
    content = json.dumps(
        table_config,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )
    return hashlib.md5(content.encode("utf-8")).hexdigest()


def _normalize_load_results(value) -> list[dict]:
    """
    Chuẩn hóa load_results nhận từ dag_run.conf.

    TriggerDagRunOperator có thể truyền:
    - list[dict]
    - tuple[dict]
    - chuỗi JSON biểu diễn list[dict]
    """
    if isinstance(value, (list, tuple)):
        return [
            item
            for item in value
            if isinstance(item, dict)
        ]

    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [
                    item
                    for item in parsed
                    if isinstance(item, dict)
                ]
        except json.JSONDecodeError:
            logger.warning(
                "load_results không phải JSON hợp lệ: %s",
                value,
            )

    return []


def _unexpected_samples(result: dict) -> list[dict]:
    """
    Chuyển unexpected values của Great Expectations
    thành dữ liệu JSON-safe để lưu database.
    """
    raw_result = result.get("raw_result", {})
    details = raw_result.get("result", {})

    unexpected_values = (
        details.get("partial_unexpected_list")
        or details.get("unexpected_list")
        or []
    )

    return [
        {
            "column_name": result.get("column_name"),
            "unexpected_value": value,
        }
        for value in unexpected_values
    ]


def resolve_el_dq_targets(
    expected_source_group: str,
    **context,
) -> list[dict]:
    """
    Đọc dag_run.conf và trả về danh sách staging table
    cần kiểm tra bằng Dynamic Task Mapping.

    dag_run.conf dự kiến:

    {
        "parent_dag_id": "el_database_pipeline",
        "parent_dag_run_id": "...",
        "source_group": "database",
        "load_results": [
            {
                "source_id": "hr_employee",
                "source_group": "database",
                "target_table": "staging.hr_employee",
                "status": "loaded",
                "batch_id": "...",
                "minio_path": "s3://...",
                "row_count": 100
            }
        ]
    }
    """
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}

    source_group = conf.get("source_group")

    if not source_group:
        raise AirflowException(
            "dag_run.conf thiếu source_group"
        )

    if source_group != expected_source_group:
        raise AirflowException(
            "DQ DAG không khớp source group: "
            f"expected={expected_source_group}, "
            f"received={source_group}"
        )

    load_results = _normalize_load_results(
        conf.get("load_results", [])
    )

    if not load_results:
        logger.warning(
            "Không có loaded staging table cần kiểm tra: "
            "source_group=%s, parent_dag_id=%s, "
            "parent_dag_run_id=%s",
            source_group,
            conf.get("parent_dag_id"),
            conf.get("parent_dag_run_id"),
        )
        return []

    mapped_targets = resolve_staging_targets(
        source_group=source_group,
        load_results=load_results,
    )

    logger.info(
        "Resolved %s staging DQ targets: "
        "source_group=%s, parent_dag_id=%s, "
        "parent_dag_run_id=%s",
        len(mapped_targets),
        source_group,
        conf.get("parent_dag_id"),
        conf.get("parent_dag_run_id"),
    )

    return mapped_targets


def run_el_table_data_quality(
    load_result: dict,
    table_config: dict,
    **context,
) -> dict:
    """
    Chạy Data Quality cho một staging table.

    Critical rule failure:
    - Ghi validation run với status=failed.
    - Không raise tại mapped task.
    - Trả kết quả về task tổng hợp.
    - Task tổng hợp quyết định trạng thái cuối của DAG.

    Lỗi thực thi:
    - Ghi validation run với status=error.
    - Raise AirflowException.
    - Airflow retry riêng mapped task bị lỗi.
    """
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}

    source_id = load_result.get("source_id")
    source_group = load_result.get("source_group")
    target_table = load_result.get("target_table")
    batch_id = load_result.get("batch_id")
    load_status = load_result.get("status")

    parent_dag_id = conf.get("parent_dag_id")
    parent_dag_run_id = conf.get("parent_dag_run_id")

    configured_group = table_config.get("source_group")
    configured_table = table_config.get("target_table")

    if not source_id:
        raise AirflowException(
            "load_result thiếu source_id"
        )

    if load_status != "loaded":
        raise AirflowException(
            f"[{source_id}] Chỉ chạy DQ cho status=loaded, "
            f"received={load_status}"
        )

    if not target_table:
        raise AirflowException(
            f"[{source_id}] load_result thiếu target_table"
        )

    if not batch_id:
        raise AirflowException(
            f"[{source_id}] loaded result thiếu batch_id"
        )

    if not target_table.startswith("staging."):
        raise AirflowException(
            f"[{source_id}] EL DQ chỉ được kiểm tra "
            f"schema staging: {target_table}"
        )

    if configured_table != target_table:
        raise AirflowException(
            f"[{source_id}] target_table không khớp: "
            f"load_result={target_table}, "
            f"DQ config={configured_table}"
        )

    if configured_group != source_group:
        raise AirflowException(
            f"[{source_id}] source_group không khớp: "
            f"load_result={source_group}, "
            f"DQ config={configured_group}"
        )

    rule_config = load_staging_rule_config()
    defaults = rule_config.get("defaults", {})

    sample_limit = int(
        defaults.get("unexpected_sample_limit", 20)
    )

    validation_run_id = str(uuid4())
    owner = table_config.get("owner", "data-team")
    rules = table_config.get("rules", [])
    config_hash = _config_hash(table_config)

    started = time.perf_counter()
    records_checked = None
    run_created = False
    repository = None

    logger.info(
        "Starting EL DQ validation: "
        "source_id=%s, source_group=%s, "
        "target_table=%s, batch_id=%s, "
        "rules=%s, owner=%s",
        source_id,
        source_group,
        target_table,
        batch_id,
        len(rules),
        owner,
    )

    try:
        repository = DataQualityRepository()

        repository.create_run(
            validation_run_id=validation_run_id,
            airflow_dag_id=context["dag"].dag_id,
            airflow_dag_run_id=dag_run.run_id,
            source_id=source_id,
            target_table=target_table,
            batch_id=batch_id,
            status="running",
            config_hash=config_hash,
            dbt_model=None,
            dbt_layer=None,
            parent_dag_id=parent_dag_id,
            parent_dag_run_id=parent_dag_run_id,
        )

        run_created = True

        dataframe = load_staging_dataframe(
            target_table=target_table,
            rules=rules,
        )

        records_checked = len(dataframe)

        results = validate_dataframe(
            dataframe=dataframe,
            source_id=source_id,
            rules=rules,
            unexpected_sample_limit=sample_limit,
        )

        repository.save_rule_results(
            validation_run_id=validation_run_id,
            source_id=source_id,
            results=results,
        )

        for result in results:
            if result.get("success"):
                continue

            repository.save_unexpected_samples(
                validation_run_id=validation_run_id,
                rule_id=result["rule_id"],
                rows=_unexpected_samples(result),
                limit=sample_limit,
            )

        total_rules = len(results)

        passed_rules = sum(
            1
            for result in results
            if result.get("success")
        )

        failed_results = [
            result
            for result in results
            if not result.get("success")
        ]

        failed_rules = len(failed_results)

        failed_severities = set(
            table_config.get(
                "fail_dag_on_severity",
                defaults.get(
                    "fail_dag_on_severity",
                    ["critical"],
                ),
            )
        )

        failed_severities = {
            str(severity).lower()
            for severity in failed_severities
        }

        critical_failures = [
            result
            for result in failed_results
            if str(
                result.get("severity", "")
            ).lower() in failed_severities
        ]

        if critical_failures:
            status = "failed"
        elif failed_rules == 0:
            status = "passed"
        else:
            status = "passed_with_warnings"

        duration_seconds = round(
            time.perf_counter() - started,
            2,
        )

        repository.finish_run(
            validation_run_id=validation_run_id,
            status=status,
            total_rules=total_rules,
            passed_rules=passed_rules,
            failed_rules=failed_rules,
            records_checked=records_checked,
            duration_seconds=duration_seconds,
        )

        result_summary = {
            "validation_run_id": validation_run_id,
            "source_id": source_id,
            "source_group": source_group,
            "target_table": target_table,
            "batch_id": batch_id,
            "minio_path": load_result.get("minio_path"),
            "loaded_row_count": load_result.get("row_count"),
            "owner": owner,
            "status": status,
            "total_rules": total_rules,
            "passed_rules": passed_rules,
            "failed_rules": failed_rules,
            "records_checked": records_checked,
            "duration_seconds": duration_seconds,
            "critical_failures": [
                result["rule_id"]
                for result in critical_failures
            ],
            "parent_dag_id": parent_dag_id,
            "parent_dag_run_id": parent_dag_run_id,
        }

        logger.info(
            "EL DQ completed: "
            "source_id=%s, target_table=%s, "
            "batch_id=%s, status=%s, "
            "passed_rules=%s, failed_rules=%s, "
            "records_checked=%s, duration=%s seconds",
            source_id,
            target_table,
            batch_id,
            status,
            passed_rules,
            failed_rules,
            records_checked,
            duration_seconds,
        )

        return result_summary

    except Exception as error:
        duration_seconds = round(
            time.perf_counter() - started,
            2,
        )

        logger.exception(
            "EL DQ execution error: "
            "source_id=%s, target_table=%s, "
            "batch_id=%s",
            source_id,
            target_table,
            batch_id,
        )

        if repository is not None and run_created:
            try:
                repository.finish_run(
                    validation_run_id=validation_run_id,
                    status="error",
                    total_rules=0,
                    passed_rules=0,
                    failed_rules=0,
                    error_message=str(error),
                    records_checked=records_checked,
                    duration_seconds=duration_seconds,
                )
            except Exception:
                logger.exception(
                    "Không thể cập nhật status=error "
                    "cho validation_run_id=%s",
                    validation_run_id,
                )

        raise AirflowException(
            f"EL Data Quality execution error "
            f"for {target_table}: {error}"
        ) from error

    finally:
        elapsed = round(
            time.perf_counter() - started,
            2,
        )

        logger.info(
            "EL DQ task finished: "
            "source_id=%s, table=%s, "
            "batch_id=%s, elapsed=%s seconds",
            source_id,
            target_table,
            batch_id,
            elapsed,
        )


def summarize_el_data_quality(
    table_results,
    **context,
) -> dict:
    """
    Tổng hợp kết quả của toàn bộ mapped table task.

    Chỉ task tổng hợp này fail khi có critical rule failure.
    Các bảng đã kiểm tra thành công không bị chạy lại.
    """
    results = list(table_results or [])

    dag_run = context["dag_run"]
    conf = dag_run.conf or {}

    source_group = conf.get("source_group")
    parent_dag_id = conf.get("parent_dag_id")
    parent_dag_run_id = conf.get("parent_dag_run_id")

    if not results:
        logger.warning(
            "Không có staging table nào được kiểm tra: "
            "source_group=%s, parent_dag_id=%s, "
            "parent_dag_run_id=%s",
            source_group,
            parent_dag_id,
            parent_dag_run_id,
        )

        return {
            "status": "no_loaded_tables",
            "source_group": source_group,
            "parent_dag_id": parent_dag_id,
            "parent_dag_run_id": parent_dag_run_id,
            "validated_tables": 0,
            "passed_tables": 0,
            "warning_tables": 0,
            "failed_tables": 0,
            "total_rules": 0,
            "passed_rules": 0,
            "failed_rules": 0,
            "records_checked": 0,
            "tables": [],
        }

    passed_tables = [
        result
        for result in results
        if result.get("status") == "passed"
    ]

    warning_tables = [
        result
        for result in results
        if result.get("status")
        == "passed_with_warnings"
    ]

    failed_tables = [
        result
        for result in results
        if result.get("status") == "failed"
    ]

    total_rules = sum(
        result.get("total_rules", 0)
        for result in results
    )

    passed_rules = sum(
        result.get("passed_rules", 0)
        for result in results
    )

    failed_rules = sum(
        result.get("failed_rules", 0)
        for result in results
    )

    records_checked = sum(
        result.get("records_checked", 0) or 0
        for result in results
    )

    summary = {
        "status": (
            "failed"
            if failed_tables
            else "passed_with_warnings"
            if warning_tables
            else "passed"
        ),
        "source_group": source_group,
        "parent_dag_id": parent_dag_id,
        "parent_dag_run_id": parent_dag_run_id,
        "validated_tables": len(results),
        "passed_tables": len(passed_tables),
        "warning_tables": len(warning_tables),
        "failed_tables": len(failed_tables),
        "total_rules": total_rules,
        "passed_rules": passed_rules,
        "failed_rules": failed_rules,
        "records_checked": records_checked,
        "tables": results,
    }

    logger.info(
        "EL DQ summary: "
        "source_group=%s, validated=%s, "
        "passed=%s, warnings=%s, failed=%s, "
        "total_rules=%s, failed_rules=%s, "
        "records_checked=%s",
        source_group,
        summary["validated_tables"],
        summary["passed_tables"],
        summary["warning_tables"],
        summary["failed_tables"],
        total_rules,
        failed_rules,
        records_checked,
    )

    if failed_tables:
        critical_failure_details = []

        for result in failed_tables:
            source_id = result.get(
                "source_id",
                "unknown_source",
            )

            target_table = result.get(
                "target_table",
                "unknown_table",
            )

            critical_rules = result.get(
                "critical_failures",
                [],
            )

            if critical_rules:
                critical_failure_details.extend(
                    (
                        f"{source_id}:"
                        f"{target_table}.{rule_id}"
                    )
                    for rule_id in critical_rules
                )
            else:
                critical_failure_details.append(
                    f"{source_id}:{target_table}"
                )

        raise AirflowException(
            "EL Data Quality critical failure: "
            + ", ".join(critical_failure_details)
        )

    return summary