"""Offline fixture extractor used only for local end-to-end reconstruction."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import pandas as pd

from src.extractors.base import BaseExtractor, ExtractResult

_STAGING_META = {"_source_id", "_source_connection", "_batch_id", "_loaded_at", "_wm"}
_ELASTIC_TIMESTAMP_SUFFIXES = ("At", "Date", "Time", "Utc", "UTC")
PROJECT_ROOT = Path(os.environ.get("DWH_PROJECT_ROOT", Path(__file__).resolve().parents[2]))


def _read_fixture(path: Path, source_type: str) -> pd.DataFrame:
    """Read a local fixture with source-faithful dtypes where they matter.

    Most non-SQL fixtures are intentionally read as strings because their live
    connectors are spreadsheet/CSV-oriented and staging models already expect
    textual values. Elasticsearch is different: live JSON responses preserve
    numeric values and ElasticExtractor converts timestamp-like fields to UTC
    datetimes. Reproduce those semantics here so local staging has the same
    PostgreSQL types as the live EL path.
    """
    if source_type == "elastic":
        df = pd.read_csv(path, keep_default_na=True, low_memory=False)
        for column in df.columns:
            if any(column.endswith(suffix) for suffix in _ELASTIC_TIMESTAMP_SUFFIXES):
                df[column] = pd.to_datetime(df[column], utc=True, errors="coerce")
        return df

    return pd.read_csv(path, dtype=str, keep_default_na=False, low_memory=False)


class FixtureExtractor(BaseExtractor):
    def extract(self, watermark_filter: Optional[str] = None) -> ExtractResult:
        cfg = self.source_config
        path = Path(cfg["fixture_csv"])
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        path = path.resolve()
        if not path.exists():
            raise FileNotFoundError(f"[{cfg['source_id']}] fixture_csv không tồn tại: {path}")

        df = _read_fixture(path, cfg.get("source_type", ""))
        df = df.drop(columns=[c for c in _STAGING_META if c in df.columns], errors="ignore")
        df["_source_id"] = cfg["source_id"]
        return ExtractResult(
            dataframe=df,
            row_count=len(df),
            checksum=None,
            watermark_value=None,
            source_meta={"fixture": str(path), "local_fixture_mode": True},
        )

    def has_changed(self, last) -> bool:
        return True
