"""
dags/dbt_run_dag.py
DAG dbt transform với giao diện chọn model/layer trong Airflow UI.

Chế độ trigger thủ công:
  - Chọn layer (silver/gold/all) hoặc model cụ thể
  - Bật/tắt chờ EL DAGs
  - Bật/tắt dbt test

Lịch tự động (7h30): chạy toàn bộ, chờ đủ 4 EL DAGs.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.models.param import Param

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

PROJECT_ROOT = os.environ.get(
    "DWH_PROJECT_ROOT",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
)
DBT_DIR = os.path.join(PROJECT_ROOT, "dwh_dbt")
DBT_BIN = os.environ.get("DBT_BIN", "dbt")

# Danh sách tất cả models
SILVER_DIM_MODELS = [
    "dim_ar", "dim_artist", "dim_artist_distro", "dim_channel", "dim_company",
    "dim_company_stock", "dim_department", "dim_distributed_employee", "dim_editing",
    "dim_isrc", "dim_khsx", "dim_net", "dim_order_employee", "dim_partners",
    "dim_platform", "dim_po", "dim_project", "dim_project_stock",
    "dim_purchased_resource", "dim_repository", "dim_resource",
    "dim_resource_before_odoo", "dim_so", "dim_stock", "dim_sub_project",
    "dim_subproject_stock", "dim_usd", "dim_video",
]

SILVER_FACT_MODELS = [
    "bridge_bt_vid", "fact_artist_assignment", "fact_distribution", "fact_editing",
    "fact_evaluation_assignment", "fact_khsx_detail", "fact_label_operation",
    "fact_music_evaluation", "fact_po_detail", "fact_purchase_cost",
    "fact_revenue_by_resources", "fact_revenue_distro", "fact_revenue_yt",
    "fact_so_detail", "fact_view_stream_distro", "fact_view_yt",
    "fact_youtube_operation",
]

GOLD_MODELS = [
    "int_stock_video", "mart_resource_channel", "mart_team_usage",
]

ALL_MODELS = SILVER_DIM_MODELS + SILVER_FACT_MODELS + GOLD_MODELS

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="dbt_transform_pipeline",
    schedule="30 7 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["transform", "dbt"],
    params={
        "layer": Param(
            default="all",
            type="string",
            title="Chọn layer cần chạy",
            description=(
                "all     → chạy toàn bộ silver + gold\n"
                "silver  → chỉ chạy silver (dim + fact)\n"
                "silver_dim  → chỉ chạy dim tables\n"
                "silver_fact → chỉ chạy fact tables\n"
                "gold    → chỉ chạy gold (mart)\n"
                "custom  → chọn model cụ thể bên dưới"
            ),
            enum=["all", "silver", "silver_dim", "silver_fact", "gold", "custom"],
        ),
        "selected_models": Param(
            default=[],
            type="array",
            title="Model cụ thể (chỉ dùng khi layer=custom)",
            description=(
                "Nhập tên model muốn chạy. Chỉ có hiệu lực khi layer=custom.\n\n"
                f"Silver dim: {', '.join(SILVER_DIM_MODELS)}\n\n"
                f"Silver fact: {', '.join(SILVER_FACT_MODELS)}\n\n"
                f"Gold: {', '.join(GOLD_MODELS)}"
            ),
        ),
        "wait_for_el": Param(
            default=True,
            type="boolean",
            title="Chờ EL DAGs hoàn tất?",
            description=(
                "True  → chờ đủ 4 EL DAGs (google_sheet, database, csv, elastic) trước khi chạy dbt.\n"
                "False → chạy dbt ngay, không chờ EL."
            ),
        ),
        "run_tests": Param(
            default=True,
            type="boolean",
            title="Chạy dbt test sau khi run?",
            description="True → chạy dbt test sau dbt run. False → bỏ qua test.",
        ),
    },
) as dag:

    def build_dbt_selector(**context):
        """Build dbt --select argument từ params."""
        layer = context["params"].get("layer", "all")
        selected_models = context["params"].get("selected_models", [])

        if layer == "all":
            return ""  # chạy tất cả
        elif layer == "silver":
            return "--select silver"
        elif layer == "silver_dim":
            return "--select silver.dim"
        elif layer == "silver_fact":
            return "--select silver.fact"
        elif layer == "gold":
            return "--select gold"
        elif layer == "custom" and selected_models:
            return f"--select {' '.join(selected_models)}"
        else:
            return ""

    def decide_wait(**context):
        """Branch: có chờ EL DAGs không?"""
        wait = context["params"].get("wait_for_el", True)
        return "wait_el_sensors" if wait else "prepare_dbt_selector"

    branch = BranchPythonOperator(
        task_id="decide_wait_for_el",
        python_callable=decide_wait,
    )

    # Sensors chờ EL DAGs
    from airflow.utils.task_group import TaskGroup
    with TaskGroup("wait_el_sensors") as wait_group:
        wait_google_sheet = ExternalTaskSensor(
            task_id="wait_el_google_sheet",
            external_dag_id="el_google_sheet_pipeline",
            timeout=3600,
            poke_interval=60,
            mode="reschedule",
        )
        wait_database = ExternalTaskSensor(
            task_id="wait_el_database",
            external_dag_id="el_database_pipeline",
            timeout=3600,
            poke_interval=60,
            mode="reschedule",
        )
        wait_api = ExternalTaskSensor(
            task_id="wait_el_api",
            external_dag_id="el_api_pipeline",
            timeout=3600,
            poke_interval=60,
            mode="reschedule",
        )
        wait_elastic = ExternalTaskSensor(
            task_id="wait_el_elastic",
            external_dag_id="el_elastic_pipeline",
            timeout=7200,
            poke_interval=60,
            mode="reschedule",
        )

    def _prepare_selector(**context):
        selector = build_dbt_selector(**context)
        context["ti"].xcom_push(key="dbt_selector", value=selector)
        layer = context["params"].get("layer", "all")
        print(f"🎯 Layer: {layer} | Selector: '{selector or '(tất cả)'}'")

    prepare_selector = PythonOperator(
        task_id="prepare_dbt_selector",
        python_callable=_prepare_selector,
        trigger_rule="none_failed_min_one_success",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT_BIN} run --profiles-dir {DBT_DIR} "
            "{{ ti.xcom_pull(task_ids='prepare_dbt_selector', key='dbt_selector') or '' }}"
        ),
        env={
            **os.environ,
            "DWH_PG_HOST":     os.environ.get("DWH_PG_HOST", "localhost"),
            "DWH_PG_PORT":     os.environ.get("DWH_PG_PORT", "5432"),
            "DWH_PG_USER":     os.environ.get("DWH_PG_USER", "dev"),
            "DWH_PG_PASSWORD": os.environ.get("DWH_PG_PASSWORD", "Inda1234"),
            "DWH_PG_DB":       os.environ.get("DWH_PG_DB", "data_warehouse"),
        },
    )

    def decide_test(**context):
        run_tests = context["params"].get("run_tests", True)
        return "dbt_test" if run_tests else "skip_test"

    branch_test = BranchPythonOperator(
        task_id="decide_run_test",
        python_callable=decide_test,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT_BIN} test --profiles-dir {DBT_DIR} "
            "{{ ti.xcom_pull(task_ids='prepare_dbt_selector', key='dbt_selector') or '' }}"
        ),
    )

    skip_test = PythonOperator(
        task_id="skip_test",
        python_callable=lambda: print("⏭ Bỏ qua dbt test theo yêu cầu."),
    )

    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality_dbt",
        trigger_dag_id="data_quality_dbt_pipeline",
        wait_for_completion=False,
        reset_dag_run=False,
        conf={
            "parent_dag_id": "{{ dag.dag_id }}",
            "parent_dag_run_id": "{{ dag_run.run_id }}",
            "layer": "{{ params.layer }}",
            "selected_models": "{{ params.selected_models | tojson }}",
        },
        trigger_rule="none_failed_min_one_success",
    )

    # Workflow
    branch >> [wait_group, prepare_selector]
    wait_group >> prepare_selector
    prepare_selector >> dbt_run >> branch_test >> [dbt_test, skip_test]
    [dbt_test, skip_test] >> trigger_data_quality
