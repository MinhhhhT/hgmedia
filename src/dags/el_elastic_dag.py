"""
src/dags/el_elastic_dag.py

EL DAG cho nhóm nguồn Elasticsearch:
Elasticsearch → MinIO → staging → trigger Elasticsearch Data Quality.
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


SOURCE_GROUP = "elastic"
DQ_DAG_ID = "data_quality_el_elastic_pipeline"


ALL_SOURCES = [
    "channel_video_info",
    "channel_video_metric",
]


SOURCE_DESCRIPTIONS = {
    "channel_video_info": (
        "ES index channel-video-info"
    ),
    "channel_video_metric": (
        "ES index channel-video-metric-*"
    ),
}


default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="el_elastic_pipeline",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_tasks=4,
    tags=[
        "el",
        "elasticsearch",
        "minio",
        "staging",
    ],
    params={
        "selected_tables": Param(
            default="all",
            type="string",
            title="Chọn Elasticsearch index",
            description=(
                "'all' để chạy tất cả.\n\n"
                + "\n".join(
                    f"{source}: {description}"
                    for source, description
                    in SOURCE_DESCRIPTIONS.items()
                )
            ),
            enum=[
                "all",
                *ALL_SOURCES,
            ],
        ),
        "date_from": Param(
            default="",
            type=["string", "null"],
            title="Ngày bắt đầu",
            description=(
                "Override date_from trong config. "
                "Định dạng YYYY-MM-DD. "
                "Để trống để dùng giá trị mặc định."
            ),
        ),
    },
)
def el_elastic_pipeline():
    @task
    def get_sources(**context):
        os.chdir(PROJECT_ROOT)

        from src.config_loader import load_sources

        selected_table = context[
            "params"
        ].get(
            "selected_tables",
            "all",
        )

        date_from = context[
            "params"
        ].get(
            "date_from",
            "",
        )

        all_sources = load_sources(
            "config/elastic_sources.yaml",
            "elastic_sources",
        )

        if selected_table == "all":
            filtered = all_sources
        else:
            filtered = [
                source
                for source in all_sources
                if source["source_id"]
                == selected_table
            ]

        if date_from:
            for source in filtered:
                source["date_from"] = date_from

            print(
                f"Override date_from={date_from}"
            )

        print(
            f"Sẽ chạy {len(filtered)}/"
            f"{len(all_sources)} Elasticsearch source:"
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
            "Elasticsearch"
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
        results = list(load_results)

        loaded_results = [
            result
            for result in results
            if result.get("status") == "loaded"
        ]

        print(
            "Chuẩn bị trigger Elasticsearch DQ: "
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
        task_id="trigger_data_quality_elastic",
        trigger_dag_id=DQ_DAG_ID,
        conf=dq_conf,
        wait_for_completion=False,
        reset_dag_run=False,
    )

    dq_conf >> trigger_dq


el_elastic_pipeline()