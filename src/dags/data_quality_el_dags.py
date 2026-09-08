import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import (
    PythonOperator,
)


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


def resolve_targets_callable(
    expected_source_group: str,
    **context,
):
    from src.tasks.el_data_quality_task import (
        resolve_el_dq_targets,
    )

    return resolve_el_dq_targets(
        expected_source_group=(
            expected_source_group
        ),
        **context,
    )


def validate_table_callable(
    load_result: dict,
    table_config: dict,
    **context,
):
    from src.tasks.el_data_quality_task import (
        run_el_table_data_quality,
    )

    return run_el_table_data_quality(
        load_result=load_result,
        table_config=table_config,
        **context,
    )


def summarize_callable(
    table_results,
    **context,
):
    from src.tasks.el_data_quality_task import (
        summarize_el_data_quality,
    )

    return summarize_el_data_quality(
        table_results=table_results,
        **context,
    )


def build_el_dq_dag(
    dag_id: str,
    source_group: str,
    max_active_tasks: int,
) -> DAG:
    with DAG(
        dag_id=dag_id,
        schedule=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        max_active_tasks=max_active_tasks,
        default_args={
            "owner": "data-team",
            "retries": 0,
        },
        tags=[
            "data-quality",
            "great-expectations",
            "staging",
            source_group,
        ],
    ) as dag:
        resolve_targets = PythonOperator(
            task_id="resolve_target_tables",
            python_callable=(
                resolve_targets_callable
            ),
            op_kwargs={
                "expected_source_group": (
                    source_group
                ),
            },
        )

        validate_tables = (
            PythonOperator.partial(
                task_id=(
                    "validate_staging_table"
                ),
                python_callable=(
                    validate_table_callable
                ),
                retries=1,
                retry_delay=timedelta(
                    minutes=2
                ),
            ).expand(
                op_kwargs=resolve_targets.output
            )
        )

        summarize = PythonOperator(
            task_id="summarize_data_quality",
            python_callable=summarize_callable,
            op_kwargs={
                "table_results": (
                    validate_tables.output
                ),
            },
            trigger_rule="none_failed",
        )

        (
            resolve_targets
            >> validate_tables
            >> summarize
        )

    return dag


data_quality_el_database_pipeline = (
    build_el_dq_dag(
        dag_id=(
            "data_quality_el_database_pipeline"
        ),
        source_group="database",
        max_active_tasks=4,
    )
)

data_quality_el_google_sheet_pipeline = (
    build_el_dq_dag(
        dag_id=(
            "data_quality_el_google_sheet_pipeline"
        ),
        source_group="google_sheet",
        max_active_tasks=2,
    )
)

data_quality_el_elastic_pipeline = (
    build_el_dq_dag(
        dag_id=(
            "data_quality_el_elastic_pipeline"
        ),
        source_group="elastic",
        max_active_tasks=1,
    )
)

data_quality_el_api_pipeline = (
    build_el_dq_dag(
        dag_id="data_quality_el_api_pipeline",
        source_group="api",
        max_active_tasks=1,
    )
)