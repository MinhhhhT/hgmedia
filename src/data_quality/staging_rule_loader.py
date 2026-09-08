from copy import deepcopy
from pathlib import Path

import yaml

from src.config_loader import load_sources


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RULES_PATH = (
    PROJECT_ROOT
    / "config"
    / "data_quality_staging_rules.yaml"
)

SOURCE_CONFIGS = {
    "database": (
        PROJECT_ROOT / "config" / "db_sources.yaml",
        "db_sources",
    ),
    "google_sheet": (
        PROJECT_ROOT
        / "config"
        / "google_sheet_sources.yaml",
        "google_sheet_sources",
    ),
    "elastic": (
        PROJECT_ROOT
        / "config"
        / "elastic_sources.yaml",
        "elastic_sources",
    ),
    "api": (
        PROJECT_ROOT / "config" / "api_sources.yaml",
        "api_sources",
    ),
}


def load_staging_rule_config() -> dict:
    if not RULES_PATH.exists():
        raise FileNotFoundError(
            f"Không tìm thấy staging DQ rules: {RULES_PATH}"
        )

    with RULES_PATH.open(
        "r",
        encoding="utf-8",
    ) as file:
        return yaml.safe_load(file) or {}


def load_canonical_sources(
    source_group: str,
) -> dict[str, dict]:
    if source_group not in SOURCE_CONFIGS:
        raise ValueError(
            f"Source group không hợp lệ: {source_group}"
        )

    config_path, config_key = SOURCE_CONFIGS[
        source_group
    ]

    sources = load_sources(
        str(config_path),
        config_key,
    )

    return {
        source["source_id"]: source
        for source in sources
    }


def resolve_staging_targets(
    source_group: str,
    load_results: list[dict],
) -> list[dict]:
    config = load_staging_rule_config()
    defaults = config.get("defaults", {})
    groups = config.get("groups", {})
    source_overrides = config.get("sources", {})

    group_config = groups.get(source_group)

    if not group_config:
        raise ValueError(
            f"Chưa có DQ config cho group: {source_group}"
        )

    if not group_config.get("enabled", True):
        return []

    canonical_sources = load_canonical_sources(
        source_group
    )

    targets = []

    for load_result in load_results:
        if load_result.get("status") != "loaded":
            continue

        source_id = load_result.get("source_id")

        if source_id not in canonical_sources:
            raise ValueError(
                f"Source {source_id} không thuộc "
                f"group {source_group}"
            )

        canonical_source = canonical_sources[source_id]

        canonical_table = canonical_source.get(
            "target_staging_table"
        )

        result_table = load_result.get(
            "target_table"
        )

        if canonical_table != result_table:
            raise ValueError(
                f"[{source_id}] target table không khớp: "
                f"EL={result_table}, "
                f"config={canonical_table}"
            )

        if not canonical_table.startswith(
            "staging."
        ):
            raise ValueError(
                f"[{source_id}] DQ EL chỉ được chạy "
                f"trên staging: {canonical_table}"
            )

        override = source_overrides.get(
            source_id,
            {},
        )

        if not override.get("enabled", True):
            continue

        base_rules = deepcopy(
            group_config.get("rules", [])
        )

        additional_rules = deepcopy(
            override.get("additional_rules", [])
        )

        table_config = {
            "enabled": True,
            "source_group": source_group,
            "owner": override.get(
                "owner",
                group_config.get(
                    "owner",
                    "data-team",
                ),
            ),
            "target_table": canonical_table,
            "rules": base_rules + additional_rules,
            "fail_dag_on_severity": (
                override.get(
                    "fail_dag_on_severity",
                    group_config.get(
                        "fail_dag_on_severity",
                        defaults.get(
                            "fail_dag_on_severity",
                            ["critical"],
                        ),
                    ),
                )
            ),
        }

        targets.append({
            "load_result": load_result,
            "table_config": table_config,
        })

    return targets