"""
src/dags/el_database_dag.py

EL DAG cho nhóm nguồn Database:
Database → MinIO → staging → trigger Database Data Quality.
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


SOURCE_GROUP = "database"
DQ_DAG_ID = "data_quality_el_database_pipeline"


ALL_SOURCES_BY_CONNECTION = {
    "odoo_pg": [
        "hr_employee",
        "purchase_order",
        "purchase_order_line",
        "res_partner",
        "sale_order",
        "sale_order_line",
        "x_acceptance_cert",
        "x_music_plan",
        "x_music_plan_detail",
        "x_music_plan_detail_price",
        "x_music_song",
        "x_product_genre",
        "x_product_subgenre",
        "x_project",
        "res_company",
        "res_users",
        "hr_department",
        "hr_job",
    ],
    "hg_stock": [
        "distribution_media_history",
        "groups",
        "resource_file_info",
        "resource_folders",
        "resource_file_action",
        "roles",
        "user_departments",
        "users",
        "tracking_video_publish_infos",
        "resource_storage_history",
        "resource_files",
    ],
    "channel_organization": [
        "company",
        "department",
        "departmentlevel",
        "position",
        "user",
        "userdepartmentposition",
    ],
    "channel_relationship": [
        "channeldepartment",
        "channel_project",
        "channel_deal",
        "channel_user",
        "channel_company",
    ],
    "channel_channel": [
        "channel",
    ],
    "channel_project": [
        "project",
    ],
    "channel_network": [
        "network",
        "cms",
    ],
    "editing_management": [
        "editings",
        "user_editing",
        "resource",
        "resource_editings",
    ],
    "record_survey": [
        "review",
        "review_user",
        "recording_sound",
        "recording_sound_version",
        "user_comment_version",
        "version_needs_comment",
    ],
}


ALL_SOURCES = [
    source_id
    for sources in ALL_SOURCES_BY_CONNECTION.values()
    for source_id in sources
]

ALL_CONNECTIONS = list(
    ALL_SOURCES_BY_CONNECTION.keys()
)


default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="el_database_pipeline",
    schedule="0 */4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_tasks=8,
    tags=[
        "el",
        "database",
        "minio",
        "staging",
    ],
    params={
        "selected_connections": Param(
            default=ALL_CONNECTIONS,
            type="array",
            title="Lọc theo Database Connection",
            description=(
                "Chọn connection nguồn cần chạy. "
                "Mặc định chạy tất cả."
            ),
            examples=ALL_CONNECTIONS,
            items={
                "type": "string",
                "enum": ALL_CONNECTIONS,
            },
        ),
        "selected_tables": Param(
            default=[],
            type="array",
            title="Chọn bảng cụ thể",
            description=(
                "Để trống để chạy tất cả bảng thuộc "
                "các connection đã chọn."
            ),
        ),
    },
)
def el_database_pipeline():
    @task
    def get_sources(**context):
        os.chdir(PROJECT_ROOT)

        from src.config_loader import load_sources

        selected_connections = context[
            "params"
        ].get(
            "selected_connections",
            ALL_CONNECTIONS,
        )

        selected_tables = context[
            "params"
        ].get(
            "selected_tables",
            [],
        )

        all_sources = load_sources(
            "config/db_sources.yaml",
            "db_sources",
        )

        filtered = [
            source
            for source in all_sources
            if source.get("connection")
            in selected_connections
        ]

        if selected_tables:
            filtered = [
                source
                for source in filtered
                if source["source_id"]
                in selected_tables
            ]

        print(
            f"Sẽ chạy {len(filtered)}/"
            f"{len(all_sources)} database tables:"
        )

        for source in filtered:
            print(
                f"  [{source.get('connection')}] "
                f"{source['source_id']} → "
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
        connection = source_config.get(
            "connection",
            "unknown",
        )
        target_table = source_config[
            "target_staging_table"
        ]

        print(
            f"[{connection}][{source_id}] "
            "Bắt đầu extract"
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
            "Chuẩn bị trigger Database DQ: "
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
        task_id="trigger_data_quality_database",
        trigger_dag_id=DQ_DAG_ID,
        conf=dq_conf,
        wait_for_completion=False,
        reset_dag_run=False,
    )

    dq_conf >> trigger_dq


el_database_pipeline()