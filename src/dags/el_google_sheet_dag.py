"""
src/dags/el_google_sheet_dag.py

EL DAG cho nhóm nguồn Google Sheet:
Google Sheet → MinIO → staging → trigger Google Sheet Data Quality.
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


SOURCE_GROUP = "google_sheet"
DQ_DAG_ID = "data_quality_el_google_sheet_pipeline"


ALL_SOURCES = [
    "partners",
    "purchased_resource",
    "purchase_cost",
    "resource_before_odoo",
    "resource_performance",
    "distro_infomation",
    "resource_infomation_add",
]


default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="el_google_sheet_pipeline",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_tasks=4,
    tags=[
        "el",
        "google_sheet",
        "minio",
        "staging",
    ],
    params={
        "selected_tables": Param(
            default=ALL_SOURCES,
            type="array",
            title="Chọn bảng cần chạy",
            description=(
                "Chọn một hoặc nhiều Google Sheet source. "
                "Mặc định chạy tất cả."
            ),
            examples=ALL_SOURCES,
            items={
                "type": "string",
                "enum": ALL_SOURCES,
            },
        ),
    },
)
def el_google_sheet_pipeline():
    @task
    def get_sources(**context):
        os.chdir(PROJECT_ROOT)

        from src.config_loader import load_sources

        selected_tables = context[
            "params"
        ].get(
            "selected_tables",
            ALL_SOURCES,
        )

        all_sources = load_sources(
            "config/google_sheet_sources.yaml",
            "google_sheet_sources",
        )

        filtered = [
            source
            for source in all_sources
            if source["source_id"]
            in selected_tables
        ]

        print(
            f"Sẽ chạy {len(filtered)}/"
            f"{len(all_sources)} Google Sheet source:"
        )

        for source in filtered:
            print(
                f"  {source['source_id']} → "
                f"{source['target_staging_table']}"
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
            f"[{source_id}] Bắt đầu extract "
            "Google Sheet"
        )

        extract_result = run_extract(
            source_config
        )

        if extract_result is None:
            print(
                f"[{source_id}] Skip - "
                "không có thay đổi"
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
        results = list(load_results)

        loaded_results = [
            result
            for result in results
            if result.get("status") == "loaded"
        ]

        print(
            "Chuẩn bị trigger Google Sheet DQ: "
            f"loaded={len(loaded_results)}, "
            f"total={len(results)}"
        )

        return {
            "parent_dag_id": context["dag"].dag_id,
            "parent_dag_run_id": (
                context["dag_run"].run_id
            ),
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
        task_id=(
            "trigger_data_quality_google_sheet"
        ),
        trigger_dag_id=DQ_DAG_ID,
        conf=dq_conf,
        wait_for_completion=False,
        reset_dag_run=False,
    )

    dq_conf >> trigger_dq


el_google_sheet_pipeline()