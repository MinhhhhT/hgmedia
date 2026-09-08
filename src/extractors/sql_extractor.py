"""
extractors/sql_extractor.py
Dùng chung cho MỌI nguồn DB (Odoo Postgres, HG Stock/Channel Service/Editing Management - SQL Server).
Khác nhau chỉ ở connection_name (xem connections.py) -> không cần viết class riêng cho từng DB.

Config mẫu (xem config/db_sources.yaml):
  source_id: fact_distribution
  source_type: sql
  connection: hg_stock
  source_table: distribution_media_history
  schema: dbo
  watermark_column: Distribution_Date
  incremental: true
"""
from typing import Optional
import gc
import pandas as pd
from sqlalchemy import create_engine, text

from src.extractors.base import BaseExtractor, ExtractResult
from src.connections import get_sqlalchemy_uri


class SQLExtractor(BaseExtractor):

    def __init__(self, source_config: dict, connection_config: dict):
        super().__init__(source_config)
        self.connection_config = connection_config
        self._engine = None

    def _get_engine(self):
        if self._engine is None:
            uri = get_sqlalchemy_uri(self.connection_config)
            kwargs = {"pool_pre_ping": True}
            if self.connection_config.get("type") == "sqlserver":
                kwargs["execution_options"] = {"isolation_level": "AUTOCOMMIT", "stream_results": True}
                kwargs["connect_args"] = {"timeout": 120}
            self._engine = create_engine(uri, **kwargs)
        return self._engine

    def _build_query(self, watermark_filter: Optional[str] = None) -> str:
        cfg = self.source_config
        schema = cfg.get("schema") or self.connection_config.get("default_schema")
        table = cfg["source_table"]
        conn_type = self.connection_config.get("type")
        if cfg.get("custom_query"):
            base_query = cfg["custom_query"]
        elif conn_type == "sqlserver":
            base_query = f"SELECT * FROM {schema}.[{table}]"
        else:
            base_query = f'SELECT * FROM {schema}."{table}"'

        mf = cfg.get("month_filter")
        if mf and cfg.get("watermark_column"):
            col = cfg["watermark_column"]
            y, m = mf.split("-")
            # range sargable -> dùng được index, KHÔNG full scan
            base_query = (f"SELECT * FROM ({base_query}) t "
                          f"WHERE t.{col} >= '{y}-{m}-01' "
                          f"AND t.{col} < DATEADD(month, 1, CAST('{y}-{m}-01' AS date))")
        if watermark_filter and cfg.get("watermark_column"):
            joiner = "WHERE" if "WHERE" not in base_query.upper() else "AND"
            base_query += f" {joiner} {cfg['watermark_column']} > CAST('{watermark_filter}' AS datetime2(3))"
        return base_query

    def extract(self, watermark_filter: Optional[str] = None) -> ExtractResult:
        cfg = self.source_config
        engine = self._get_engine()
        query = self._build_query(watermark_filter)
        import logging
        log = logging.getLogger(__name__)
        chunksize = cfg.get("chunksize", 50000)

        # stream thẳng chunk -> staging, không giữ toàn bộ trong RAM
        if cfg.get("stream_to_staging"):
            from sqlalchemy import create_engine as _ce, text
            from src.connections import get_connection, get_sqlalchemy_uri
            dwh = _ce(get_sqlalchemy_uri(get_connection("dwh_postgres")))
            schema, table = cfg["target_staging_table"].split(".")
            keys = cfg.get("upsert_key") or []
            wcol = cfg.get("watermark_column")
            total, wm = 0, None
            for i, ck in enumerate(pd.read_sql(query, engine, chunksize=chunksize), 1):
                ck["_source_id"] = cfg["source_id"]
                ck["_source_connection"] = cfg["connection"]
                ck.head(0).to_sql(table, dwh, schema=schema, index=False, if_exists="append")
                if keys and watermark_filter:      # chỉ upsert khi chạy delta
                    with dwh.begin() as conn:
                        cond = " AND ".join([f'"{k}" = :{k}' for k in keys])
                        params = [dict(r) for _, r in ck[keys].drop_duplicates().iterrows()]
                        conn.execute(text(f'DELETE FROM {schema}.{table} WHERE {cond}'), params)
                ck.to_sql(table, dwh, schema=schema, index=False, if_exists="append")
                total += len(ck)
                if wcol and wcol in ck.columns and len(ck):
                    m = str(ck[wcol].max()); wm = m if wm is None or m > wm else wm
                log.info(f"[{cfg['source_id']}] chunk {i}: +{len(ck)} (tổng {total}) upsert")
            return ExtractResult(dataframe=pd.DataFrame(), row_count=total,
                                 checksum=None, watermark_value=wm,
                                 source_meta={"query": query, "streamed": True})

        PART_THRESHOLD = 100000  # bảng > 100k dòng thì chia part

        chunks, total = [], 0
        part = 0
        minio_parts = []
        watermark_value = None

        from src.minio_client import MinIOClient
        minio = MinIOClient()
        batch_id = minio.make_batch_id(cfg["source_id"])

        for i, ck in enumerate(pd.read_sql(query, engine, chunksize=chunksize), 1):
            ck["_source_id"] = cfg["source_id"]
            ck["_source_connection"] = cfg["connection"]
            total += len(ck)

            if cfg.get("watermark_column") and cfg["watermark_column"] in ck.columns and len(ck):
                m = str(ck[cfg["watermark_column"]].max())
                watermark_value = m if watermark_value is None or m > watermark_value else watermark_value

            if total > PART_THRESHOLD or part > 0:
                # Bảng lớn → upload từng part
                part += 1
                path = minio.upload_dataframe_part(ck, cfg, batch_id, part)
                minio_parts.append(path)
                log.info(f"[{cfg['source_id']}] chunk {i}: +{len(ck)} (tổng {total}) → MinIO part {part}")
                del ck
                gc.collect()
            else:
                # Bảng nhỏ → gom vào RAM như cũ
                chunks.append(ck)
                log.info(f"[{cfg['source_id']}] chunk {i}: +{len(ck)} (tổng {total})")

        # Bảng nhỏ — upload 1 file duy nhất
        if chunks:
            df = pd.concat(chunks, ignore_index=True)
            log.info(f"[{cfg['source_id']}] Đọc xong {len(df)} dòng từ {cfg.get('source_table')}")
            return ExtractResult(
                dataframe=df, row_count=len(df), checksum=None,
                watermark_value=watermark_value,
                source_meta={"query": query, "connection": cfg["connection"]}
            )

        # Bảng lớn — trả về multi_part
        first_path = minio_parts[0] if minio_parts else ""
        return ExtractResult(
            dataframe=pd.DataFrame(), row_count=total,
            checksum=None, watermark_value=watermark_value,
            source_meta={
                "query": query,
                "connection": cfg["connection"],
                "batch_id": batch_id,
                "minio_path": first_path,
                "streamed": False,
                "multi_part": True,
            }
        )
    def has_changed(self, last_checksum_or_watermark: Optional[str]) -> bool:
        cfg = self.source_config
        if not cfg.get("incremental") or not cfg.get("watermark_column"):
            return True  # bảng không có watermark -> luôn full reload, coi như "đổi"

        engine = self._get_engine()
        schema = cfg.get("schema") or self.connection_config.get("default_schema")
        table = cfg["source_table"]
        col = cfg["watermark_column"]

        if last_checksum_or_watermark is None:
            check_query = f"SELECT COUNT(*) AS cnt FROM {schema}.{table}"
        else:
            check_query = (
                f"SELECT COUNT(*) AS cnt FROM {schema}.{table} "
                f"WHERE {col} > '{last_checksum_or_watermark}'"
            )
        with engine.connect() as conn:
            result = conn.execute(text(check_query)).fetchone()
        return result[0] > 0
