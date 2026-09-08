"""
src/dags/el_api_dag.py

EL DAG cho nguồn API:
API → MinIO → staging → trigger API Data Quality.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow.models.param import Param
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.sdk import dag, task


PROJECT_ROOT = os.environ.get(
    "DWH_PROJECT_ROOT",
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
        )
    ),
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


ALL_SOURCES = ["sale"]
SOURCE_GROUP = "api"
DQ_DAG_ID = "data_quality_el_api_pipeline"


default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="el_api_pipeline",
    schedule="0 5 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["el", "api"],
    params={
        "selected_sources": Param(
            default=ALL_SOURCES,
            type="array",
            title="Chọn API source cần chạy",
            description="Mặc định chạy tất cả API source.",
            examples=ALL_SOURCES,
            items={
                "type": "string",
                "enum": ALL_SOURCES,
            },
        ),
        "date_from": Param(
            default="",
            type=["string", "null"],
            title="Ngày bắt đầu",
            description=(
                "Override date_from trong config. "
                "Để trống để dùng giá trị mặc định."
            ),
        ),
    },
)
def el_api_pipeline():
    @task
    def get_sources(**context):
        os.chdir(PROJECT_ROOT)

        from src.config_loader import load_sources

        selected = context["params"].get(
            "selected_sources",
            ALL_SOURCES,
        )

        date_from = context["params"].get(
            "date_from",
            "",
        )

        all_sources = load_sources(
            "config/api_sources.yaml",
            "api_sources",
        )

        filtered = [
            source
            for source in all_sources
            if source["source_id"] in selected
        ]

        if date_from:
            for source in filtered:
                source["date_from"] = date_from

        print(
            f"Sẽ chạy {len(filtered)}/"
            f"{len(all_sources)} API source: "
            f"{[s['source_id'] for s in filtered]}"
        )

        return filtered

    @task
    def extract_and_load(
        source_config: dict,
    ):
        os.chdir(PROJECT_ROOT)

        from src.tasks.extract_task import run_extract
        from src.tasks.load_task import run_load

        source_id = source_config["source_id"]
        target_table = source_config[
            "target_staging_table"
        ]

        print(
            f"[{source_id}] Bắt đầu extract API"
        )

        extract_result = run_extract(
            source_config
        )

        if extract_result is None:
            print(
                f"[{source_id}] Skip - "
                "không có dữ liệu"
            )

            return {
                "source_id": source_id,
                "source_group": SOURCE_GROUP,
                "target_table": target_table,
                "status": "skipped",
                "batch_id": None,
                "minio_path": None,
                "row_count": 0,
            }

        row_count = run_load(
            source_config,
            extract_result["batch_id"],
            extract_result["minio_path"],
        )

        print(
            f"[{source_id}] Loaded "
            f"{row_count} rows → {target_table}"
        )

        return {
            "source_id": source_id,
            "source_group": SOURCE_GROUP,
            "target_table": target_table,
            "status": "loaded",
            "batch_id": extract_result["batch_id"],
            "minio_path": extract_result["minio_path"],
            "row_count": row_count,
        }

    @task
    def build_dq_conf(
        load_results,
        **context,
    ):
        loaded_results = [
            result
            for result in list(
                load_results or []
            )
            if result.get("status") == "loaded"
        ]

        return {
            "parent_dag_id": context[
                "dag"
            ].dag_id,
            "parent_dag_run_id": context[
                "dag_run"
            ].run_id,
            "source_group": SOURCE_GROUP,
            "load_results": loaded_results,
        }

    sources = get_sources()

    load_results = extract_and_load.expand(
        source_config=sources
    )

    dq_conf = build_dq_conf(
        load_results
    )

    trigger_dq = TriggerDagRunOperator(
        task_id="trigger_data_quality_api",
        trigger_dag_id=DQ_DAG_ID,
        conf=dq_conf,
        wait_for_completion=False,
        reset_dag_run=False,
    )

    dq_conf >> trigger_dq


el_api_pipeline()