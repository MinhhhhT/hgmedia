"""Select an extractor based on source configuration."""
from __future__ import annotations

import os

from src.connections import get_connection
from src.extractors.google_sheet_extractor import GoogleSheetExtractor
from src.extractors.sql_extractor import SQLExtractor


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def get_extractor(source_config: dict):
    source_type = source_config["source_type"]
    if source_type != "sql" and _truthy(os.environ.get("LOCAL_FIXTURE_MODE")) and source_config.get("fixture_csv"):
        from src.extractors.fixture_extractor import FixtureExtractor
        return FixtureExtractor(source_config)
    if source_type == "fx":
        from src.extractors.fx_extractor import FxExtractor
        return FxExtractor(source_config)
    if source_type == "csv":
        from src.extractors.csv_extractor import CsvExtractor
        return CsvExtractor(source_config)
    if source_type == "google_sheet":
        return GoogleSheetExtractor(source_config, get_connection("google_sheet"))
    if source_type == "elastic":
        from src.extractors.elastic_extractor import ElasticExtractor
        return ElasticExtractor(source_config, get_connection(source_config["connection"]))
    if source_type == "sql":
        return SQLExtractor(source_config, get_connection(source_config["connection"]))
    if source_type == "api":
        from src.extractors.api_extractor import ApiExtractor
        return ApiExtractor(source_config)
    raise ValueError(f"source_type '{source_type}' chưa được hỗ trợ")
