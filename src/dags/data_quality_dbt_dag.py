import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


PROJECT_ROOT = os.environ.get(
    "DWH_PROJECT_ROOT",
    "/mnt/d/HG_Project/etl_pipeline/dwh-pipeline-mapping/dwh-pipeline",
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


default_args = {
    "owner": "data-team",
    "retries": 0,
}


def get_dq_targets(**context):
    """
    Đọc dag_run.conf và trả về danh sách bảng cần kiểm tra.

    Import đặt bên trong callable để Airflow không import
    Great Expectations trong lúc parse DAG.
    """
    from src.tasks.dbt_data_quality_task import resolve_dq_targets

    return resolve_dq_targets(**context)


def validate_one_table(
    target_table: str,
    table_config: dict,
    **context,
):
    """
    Kiểm tra Data Quality cho đúng một bảng.
    Hàm này được Dynamic Task Mapping gọi cho từng bảng.
    """
    from src.tasks.dbt_data_quality_task import (
        run_dbt_table_data_quality,
    )

    return run_dbt_table_data_quality(
        target_table=target_table,
        table_config=table_config,
        **context,
    )


def summarize_results(table_results, **context):
    """
    Tổng hợp kết quả của tất cả mapped task và quyết định
    trạng thái cuối cùng của DAG.
    """
    from src.tasks.dbt_data_quality_task import (
        summarize_dbt_data_quality,
    )

    return summarize_dbt_data_quality(
        table_results=table_results,
        **context,
    )


with DAG(
    dag_id="data_quality_dbt_pipeline",
    description="Great Expectations checks for dbt Silver and Gold models",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["data-quality", "great-expectations", "dbt"],
) as dag:

    resolve_targets = PythonOperator(
        task_id="resolve_target_tables",
        python_callable=get_dq_targets,
        retries=0,
    )

    validate_tables = PythonOperator.partial(
        task_id="validate_dbt_table",
        python_callable=validate_one_table,
        retries=1,
        retry_delay=timedelta(minutes=2),
    ).expand(
        op_kwargs=resolve_targets.output
    )

    summarize = PythonOperator(
        task_id="summarize_data_quality",
        python_callable=summarize_results,
        op_kwargs={
            "table_results": validate_tables.output,
        },
        retries=0,
    )

    resolve_targets >> validate_tables >> summarize