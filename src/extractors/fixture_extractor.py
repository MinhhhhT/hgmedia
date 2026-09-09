"""Offline fixture extractor used only for local end-to-end reconstruction."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import pandas as pd

from src.extractors.base import BaseExtractor, ExtractResult

_STAGING_META = {"_source_id", "_source_connection", "_batch_id", "_loaded_at", "_wm"}
PROJECT_ROOT = Path(os.environ.get("DWH_PROJECT_ROOT", Path(__file__).resolve().parents[2]))


class FixtureExtractor(BaseExtractor):
    def extract(self, watermark_filter: Optional[str] = None) -> ExtractResult:
        cfg = self.source_config
        path = Path(cfg["fixture_csv"])
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        path = path.resolve()
        if not path.exists():
            raise FileNotFoundError(f"[{cfg['source_id']}] fixture_csv không tồn tại: {path}")
        df = pd.read_csv(path, dtype=str, keep_default_na=False, low_memory=False)
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
