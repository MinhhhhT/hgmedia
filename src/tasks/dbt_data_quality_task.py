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
from src.data_quality.rule_loader import (
    load_rule_config,
    resolve_target_tables,
)


logger = logging.getLogger(__name__)


def _config_hash(table_config: dict) -> str:
    """
    Tạo hash để xác định phiên bản rule config
    được sử dụng cho lần validation.
    """
    content = json.dumps(
        table_config,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )

    return hashlib.md5(
        content.encode("utf-8")
    ).hexdigest()


def _normalize_selected_models(value) -> list[str]:
    """
    Chuẩn hóa selected_models.

    TriggerDagRunOperator có thể truyền selected_models
    dưới dạng list hoặc chuỗi JSON.
    """
    if isinstance(value, list):
        return [
            str(model)
            for model in value
            if model
        ]

    if isinstance(value, str):
        try:
            parsed = json.loads(value)

            if isinstance(parsed, list):
                return [
                    str(model)
                    for model in parsed
                    if model
                ]

        except json.JSONDecodeError:
            logger.warning(
                "selected_models không phải JSON hợp lệ: %s",
                value,
            )

    return []


def _unexpected_samples(result: dict) -> list[dict]:
    """
    Chuyển partial unexpected values của GX
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


def resolve_dq_targets(**context) -> list[dict]:
    """
    Đọc dag_run.conf và trả về payload dùng cho
    Airflow Dynamic Task Mapping.

    Mỗi phần tử trong list sẽ tạo thành một mapped task.
    """
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}

    layer = conf.get("layer", "all")

    selected_models = _normalize_selected_models(
        conf.get("selected_models", [])
    )

    target_tables = resolve_target_tables(
        layer=layer,
        selected_models=selected_models,
    )

    if not target_tables:
        logger.warning(
            "Không tìm thấy table DQ phù hợp: "
            "layer=%s, selected_models=%s",
            layer,
            selected_models,
        )

        return []

    mapped_targets = [
        {
            "target_table": target_table,
            "table_config": table_config,
        }
        for target_table, table_config in target_tables
    ]

    logger.info(
        "Resolved %s Data Quality target tables "
        "for layer=%s, selected_models=%s",
        len(mapped_targets),
        layer,
        selected_models,
    )

    return mapped_targets


def run_dbt_table_data_quality(
    target_table: str,
    table_config: dict,
    **context,
) -> dict:
    """
    Chạy Data Quality cho đúng một bảng.

    Critical rule failure:
        - Ghi status=failed.
        - Trả kết quả về task tổng hợp.
        - Không retry mapped task.

    Lỗi thực thi:
        - Ghi status=error.
        - Raise AirflowException.
        - Airflow chỉ retry mapped task của bảng này.
    """
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}

    parent_dag_id = conf.get(
        "parent_dag_id",
        "dbt_transform_pipeline",
    )
    parent_dag_run_id = conf.get(
        "parent_dag_run_id"
    )

    rule_config = load_rule_config()
    defaults = rule_config.get("defaults", {})

    sample_limit = int(
        defaults.get(
            "unexpected_sample_limit",
            20,
        )
    )

    validation_run_id = str(uuid4())
    dbt_model = table_config.get("dbt_model")
    source_id = dbt_model or target_table
    rules = table_config.get("rules", [])
    owner = table_config.get("owner", "unknown")
    config_hash = _config_hash(table_config)

    started = time.perf_counter()
    records_checked = None
    run_created = False
    repository = None

    logger.info(
        "Starting Data Quality validation: "
        "target_table=%s, dbt_model=%s, rules=%s",
        target_table,
        dbt_model,
        len(rules),
    )

    try:
        repository = DataQualityRepository()

        repository.create_run(
            validation_run_id=validation_run_id,
            airflow_dag_id=context["dag"].dag_id,
            airflow_dag_run_id=dag_run.run_id,
            source_id=source_id,
            target_table=target_table,
            batch_id=None,
            status="running",
            config_hash=config_hash,
            dbt_model=dbt_model,
            dbt_layer=table_config.get("layer"),
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
            if result["success"]:
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
            if result["success"]
        )

        failed_results = [
            result
            for result in results
            if not result["success"]
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

        table_critical_failures = [
            result
            for result in failed_results
            if str(result.get("severity", "")).lower()
            in failed_severities
        ]

        if table_critical_failures:
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
            "target_table": target_table,
            "dbt_model": dbt_model,
            "owner": owner,
            "status": status,
            "total_rules": total_rules,
            "passed_rules": passed_rules,
            "failed_rules": failed_rules,
            "records_checked": records_checked,
            "duration_seconds": duration_seconds,
            "critical_failures": [
                result["rule_id"]
                for result in table_critical_failures
            ],
            "parent_dag_id": parent_dag_id,
            "parent_dag_run_id": parent_dag_run_id,
        }

        logger.info(
            "Data Quality completed: "
            "target_table=%s, status=%s, "
            "passed_rules=%s, failed_rules=%s, "
            "records_checked=%s, duration=%s seconds",
            target_table,
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
            "Data Quality execution error: "
            "target_table=%s",
            target_table,
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
            f"Data Quality execution error for "
            f"{target_table}: {error}"
        ) from error

    finally:
        elapsed = round(
            time.perf_counter() - started,
            2,
        )

        logger.info(
            "DQ task finished: table=%s, "
            "elapsed=%s seconds",
            target_table,
            elapsed,
        )


def summarize_dbt_data_quality(
    table_results,
    **context,
) -> dict:
    """
    Tổng hợp kết quả của toàn bộ mapped task.

    Chỉ task tổng hợp này fail khi có critical rule failure.
    Vì retries=0 nên không chạy lại các bảng đã hoàn thành.
    """
    results = list(table_results or [])

    if not results:
        logger.warning(
            "Không có bảng nào được kiểm tra Data Quality."
        )

        return {
            "status": "no_tables_configured",
            "validated_tables": 0,
            "passed_tables": 0,
            "warning_tables": 0,
            "failed_tables": 0,
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
        if result.get("status") == "passed_with_warnings"
    ]

    failed_tables = [
        result
        for result in results
        if result.get("status") == "failed"
    ]

    summary = {
        "status": (
            "failed"
            if failed_tables
            else "passed_with_warnings"
            if warning_tables
            else "passed"
        ),
        "validated_tables": len(results),
        "passed_tables": len(passed_tables),
        "warning_tables": len(warning_tables),
        "failed_tables": len(failed_tables),
        "tables": results,
    }

    logger.info(
        "Data Quality summary: "
        "validated=%s, passed=%s, warnings=%s, failed=%s",
        summary["validated_tables"],
        summary["passed_tables"],
        summary["warning_tables"],
        summary["failed_tables"],
    )

    if failed_tables:
        critical_failure_details = []

        for result in failed_tables:
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
                    f"{target_table}.{rule_id}"
                    for rule_id in critical_rules
                )
            else:
                critical_failure_details.append(
                    target_table
                )

        raise AirflowException(
            "Data quality critical failure: "
            + ", ".join(critical_failure_details)
        )

    return summary