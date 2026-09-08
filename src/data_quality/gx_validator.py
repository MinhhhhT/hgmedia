import json
import re
from typing import Any

import great_expectations as gx
import pandas as pd
from sqlalchemy import create_engine, text

from src.connections import (
    get_connection,
    get_sqlalchemy_uri,
)


TABLE_NAME_PATTERN = re.compile(
    r"^[a-zA-Z_][a-zA-Z0-9_]*\."
    r"[a-zA-Z_][a-zA-Z0-9_]*$"
)

COLUMN_NAME_PATTERN = re.compile(
    r"^[a-zA-Z_][a-zA-Z0-9_]*$"
)


def _to_dict(value: Any) -> dict:
    """
    Chuyển kết quả Great Expectations về dictionary.
    """
    if hasattr(value, "to_json_dict"):
        return value.to_json_dict()

    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")

    if isinstance(value, dict):
        return value

    return {
        "value": str(value),
    }


def _to_text(value: Any) -> str | None:
    """
    Chuyển observed value thành dữ liệu có thể lưu database.
    """
    if value is None:
        return None

    if isinstance(value, (dict, list)):
        return json.dumps(
            value,
            ensure_ascii=False,
            default=str,
        )

    return str(value)


def _limit_unexpected_values(
    raw_result: dict,
    limit: int,
) -> dict:
    """
    Giới hạn số unexpected values lưu trong result JSON.
    """
    result = raw_result.get("result")

    if not isinstance(result, dict):
        return raw_result

    for key in (
        "partial_unexpected_list",
        "unexpected_list",
    ):
        values = result.get(key)

        if isinstance(values, list):
            result[key] = values[:limit]

    return raw_result


def _extract_rule_columns(
    rules: list[dict],
) -> list[str]:
    """
    Lấy danh sách cột được sử dụng trong rules.

    Hỗ trợ:
    - column
    - column_A
    - column_B
    - column_list
    """
    columns = set()

    for rule in rules:
        if not rule.get("enabled", True):
            continue

        kwargs = rule.get("kwargs", {})

        for key in (
            "column",
            "column_A",
            "column_B",
        ):
            value = kwargs.get(key)

            if isinstance(value, str) and value:
                columns.add(value)

        column_list = kwargs.get("column_list", [])

        if isinstance(column_list, list):
            columns.update(
                column
                for column in column_list
                if isinstance(column, str) and column
            )

    invalid_columns = [
        column
        for column in columns
        if not COLUMN_NAME_PATTERN.match(column)
    ]

    if invalid_columns:
        raise ValueError(
            "Tên cột không hợp lệ trong Data Quality rules: "
            + ", ".join(invalid_columns)
        )

    return sorted(columns)


def load_staging_dataframe(
    target_table: str,
    rules: list[dict],
) -> pd.DataFrame:
    """
    Đọc dữ liệu cần thiết cho Data Quality.

    Không còn SELECT *.
    Chỉ đọc các cột xuất hiện trong rules.

    Nếu bảng chỉ có table-level expectation,
    sử dụng SELECT 1 để giảm độ rộng DataFrame.
    """
    if not TABLE_NAME_PATTERN.match(target_table):
        raise ValueError(
            f"Tên bảng không hợp lệ: {target_table}"
        )

    schema_name, table_name = target_table.split(
        ".",
        maxsplit=1,
    )

    qualified_table = (
        f'"{schema_name}"."{table_name}"'
    )

    rule_columns = _extract_rule_columns(rules)

    if rule_columns:
        select_columns = ", ".join(
            f'"{column}"'
            for column in rule_columns
        )
    else:
        select_columns = "1 AS _dq_row"

    query = text(
        f"SELECT {select_columns} "
        f"FROM {qualified_table}"
    )

    conn_cfg = get_connection("dwh_postgres")

    engine = create_engine(
        get_sqlalchemy_uri(conn_cfg)
    )

    try:
        dataframe = pd.read_sql(
            query,
            engine,
        )

        return dataframe

    finally:
        engine.dispose()


def _safe_gx_name(value: str) -> str:
    """
    Chuẩn hóa tên datasource/asset để tránh ký tự
    không hợp lệ trong Great Expectations.
    """
    normalized = re.sub(
        r"[^a-zA-Z0-9_]+",
        "_",
        value,
    ).strip("_")

    return normalized or "unknown_source"


def validate_dataframe(
    dataframe: pd.DataFrame,
    source_id: str,
    rules: list[dict],
    unexpected_sample_limit: int = 20,
) -> list[dict]:
    """
    Chạy Great Expectations rules trên một DataFrame.
    """
    context = gx.get_context(
        mode="ephemeral"
    )

    safe_source_id = _safe_gx_name(source_id)

    datasource = context.data_sources.add_pandas(
        name=f"dq_pandas_{safe_source_id}"
    )

    asset = datasource.add_dataframe_asset(
        name=f"dq_asset_{safe_source_id}"
    )

    batch_definition = (
        asset.add_batch_definition_whole_dataframe(
            name=f"dq_batch_{safe_source_id}"
        )
    )

    batch = batch_definition.get_batch(
        batch_parameters={
            "dataframe": dataframe,
        }
    )

    normalized_results = []

    for rule in rules:
        if not rule.get("enabled", True):
            continue

        rule_id = rule["id"]
        expectation_name = rule["expectation"]

        severity = str(
            rule.get("severity", "warning")
        ).lower()

        kwargs = rule.get("kwargs", {})
        column_name = kwargs.get("column")

        try:
            expectation_class = getattr(
                gx.expectations,
                expectation_name,
            )

            expectation = expectation_class(
                **kwargs
            )

            gx_result = batch.validate(
                expectation
            )

            raw_result = _limit_unexpected_values(
                _to_dict(gx_result),
                unexpected_sample_limit,
            )

            result_details = raw_result.get(
                "result",
                {},
            )

            success = bool(
                raw_result.get(
                    "success",
                    False,
                )
            )

            normalized_results.append({
                "rule_id": rule_id,
                "expectation_type": expectation_name,
                "column_name": column_name,
                "severity": severity,
                "success": success,
                "observed_value": _to_text(
                    result_details.get(
                        "observed_value"
                    )
                ),
                "unexpected_count": (
                    result_details.get(
                        "unexpected_count"
                    )
                ),
                "unexpected_percent": (
                    result_details.get(
                        "unexpected_percent"
                    )
                ),
                "raw_result": raw_result,
            })

        except Exception as error:
            normalized_results.append({
                "rule_id": rule_id,
                "expectation_type": expectation_name,
                "column_name": column_name,
                "severity": severity,
                "success": False,
                "observed_value": None,
                "unexpected_count": None,
                "unexpected_percent": None,
                "raw_result": {
                    "success": False,
                    "exception_message": str(error),
                },
            })

    return normalized_results