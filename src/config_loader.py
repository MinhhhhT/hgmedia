"""Load source YAML configuration with environment-variable expansion.

Supported forms in YAML string values:
  ${VAR}
  ${VAR:-default-value}

Secrets and deployment-specific endpoints therefore stay out of versioned YAML.
Legacy boolean strings such as ``false;`` are normalized defensively so an old
config typo cannot become a truthy Python string.

Relative config paths are always resolved from the project root instead of the
process working directory. This keeps CLI, Airflow and ad-hoc ``docker exec``
runs consistent.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")

# Prefer the explicit runtime project root used by Docker/Airflow. Outside the
# container, fall back to the repository root derived from this file location.
_PROJECT_ROOT = Path(
    os.environ.get("DWH_PROJECT_ROOT", str(Path(__file__).resolve().parents[1]))
).expanduser().resolve()


def _resolve_path(path_value: str | Path) -> Path:
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        path = _PROJECT_ROOT / path
    return path.resolve()


def _expand_string(value: str) -> Any:
    def repl(match: re.Match) -> str:
        key, default = match.group(1), match.group(2)
        if key in os.environ:
            return os.environ[key]
        return "" if default is None else default

    expanded = _ENV_PATTERN.sub(repl, value)
    normalized = expanded.strip().rstrip(";").strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    return expanded


def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        return _expand_string(value)
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    return value


def load_sources(yaml_path: str, key: str) -> list[dict]:
    path = _resolve_path(yaml_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Không tìm thấy config: {yaml_path} (resolved: {path})"
        )
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    return _expand_env(data).get(key, [])


def load_all_sources(config_dir: str = "config") -> list[dict]:
    config_root = _resolve_path(config_dir)
    all_sources = []
    all_sources += load_sources(config_root / "google_sheet_sources.yaml", "google_sheet_sources")
    all_sources += load_sources(config_root / "db_sources.yaml", "db_sources")
    all_sources += load_sources(config_root / "elastic_sources.yaml", "elastic_sources")
    all_sources += load_sources(config_root / "csv_sources.yaml", "csv_sources")
    all_sources += load_sources(config_root / "api_sources.yaml", "api_sources")
    return all_sources


def get_source_by_id(source_id: str, config_dir: str = "config") -> dict:
    for src in load_all_sources(config_dir):
        if src["source_id"] == source_id:
            return src
    raise ValueError(f"Không tìm thấy source_id '{source_id}' trong config")
