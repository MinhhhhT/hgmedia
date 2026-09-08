from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RULES_PATH = PROJECT_ROOT / "config" / "data_quality_rules.yaml"


def load_rule_config() -> dict:
    if not RULES_PATH.exists():
        raise FileNotFoundError(f"Không tìm thấy file DQ rules: {RULES_PATH}")

    with RULES_PATH.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def get_source_rules(source_id: str) -> tuple[dict, dict]:
    config = load_rule_config()
    defaults = config.get("defaults", {})
    source_rules = config.get("sources", {}).get(source_id, {})
    return defaults, source_rules


def resolve_target_tables(
    layer: str = "all",
    selected_models: list[str] | None = None,
) -> list[tuple[str, dict]]:
    """Return enabled DQ table configurations selected by the dbt DAG run."""
    selected_models = selected_models or []
    tables = load_rule_config().get("tables", {})
    targets = []

    for target_table, table_config in tables.items():
        if not table_config.get("enabled", True):
            continue

        dbt_model = table_config.get("dbt_model")
        table_layer = table_config.get("layer")

        if layer == "custom":
            matches = dbt_model in selected_models
        elif layer == "all":
            matches = True
        elif layer == "silver_dim":
            matches = (
                table_layer == "silver"
                and isinstance(dbt_model, str)
                and dbt_model.startswith("dim_")
            )
        elif layer == "silver_fact":
            matches = (
                table_layer == "silver"
                and isinstance(dbt_model, str)
                and dbt_model.startswith("fact_")
            )
        else:
            matches = table_layer == layer

        if matches:
            targets.append((target_table, table_config))

    return targets
