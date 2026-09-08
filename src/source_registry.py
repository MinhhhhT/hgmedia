"""
source_registry.py
Sổ cái dùng chung cho mọi loại nguồn (thay file_registry.py cũ chỉ dành cho Excel).
Lưu trong Postgres schema meta._source_registry (xem scripts/init_schemas.sql).
"""
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text

from src.connections import get_connection, get_sqlalchemy_uri


class SourceRegistry:

    def __init__(self):
        conn_cfg = get_connection("dwh_postgres")
        self.engine = create_engine(get_sqlalchemy_uri(conn_cfg))

    def get_last_success(self, source_id: str) -> Optional[dict]:
        query = text("""
            SELECT * FROM meta._source_registry
            WHERE source_id = :source_id AND status = 'loaded'
            ORDER BY extracted_at DESC LIMIT 1
        """)
        with self.engine.connect() as conn:
            row = conn.execute(query, {"source_id": source_id}).mappings().fetchone()
        return dict(row) if row else None

    def register_extracted(
        self, source_id: str, source_type: str, connection_name: Optional[str],
        batch_id: str, minio_path: str, row_count: int,
        checksum: Optional[str] = None, watermark_value: Optional[str] = None,
    ):
        query = text("""
            INSERT INTO meta._source_registry
                (source_id, source_type, connection_name, batch_id, minio_path,
                 checksum, watermark_value, row_count, status, extracted_at)
            VALUES
                (:source_id, :source_type, :connection_name, :batch_id, :minio_path,
                 :checksum, :watermark_value, :row_count, 'extracted', :extracted_at)
        """)
        with self.engine.begin() as conn:
            conn.execute(query, {
                "source_id": source_id, "source_type": source_type,
                "connection_name": connection_name, "batch_id": batch_id,
                "minio_path": minio_path, "checksum": checksum,
                "watermark_value": watermark_value, "row_count": row_count,
                "extracted_at": datetime.now(),
            })

    def mark_loaded(self, batch_id: str):
        query = text("""
            UPDATE meta._source_registry
            SET status = 'loaded', loaded_at = :loaded_at
            WHERE batch_id = :batch_id
        """)
        with self.engine.begin() as conn:
            conn.execute(query, {"batch_id": batch_id, "loaded_at": datetime.now()})

    def mark_failed(self, batch_id: str, error_message: str):
        query = text("""
            UPDATE meta._source_registry
            SET status = 'failed', error_message = :error_message
            WHERE batch_id = :batch_id
        """)
        with self.engine.begin() as conn:
            conn.execute(query, {"batch_id": batch_id, "error_message": error_message[:2000]})

    def find_batch_for_rollback(self, source_id: str, target_date: str = None) -> Optional[dict]:
        """
        Tìm batch có file MinIO để rollback:
        - target_date=None  → batch gần nhất có file MinIO
        - target_date='...' → batch gần nhất <= ngày đó có file MinIO
        Filter: bỏ qua batch streamed và batch rỗng (row_count=0)
        """
        if target_date:
            query = text("""
                SELECT * FROM meta._source_registry
                WHERE source_id = :source_id
                AND minio_path != '(streamed)'
                AND row_count > 0
                AND extracted_at::date <= :target_date
                ORDER BY extracted_at DESC LIMIT 1
            """)
            params = {"source_id": source_id, "target_date": target_date}
        else:
            query = text("""
                SELECT * FROM meta._source_registry
                WHERE source_id = :source_id
                AND minio_path != '(streamed)'
                AND row_count > 0
                ORDER BY extracted_at DESC LIMIT 1
            """)
            params = {"source_id": source_id}

        with self.engine.connect() as conn:
            row = conn.execute(query, params).mappings().fetchone()
        return dict(row) if row else None

    def history(self, source_id: str, limit: int = 20) -> pd.DataFrame:
        query = text("""
            SELECT batch_id, source_type, connection_name, row_count, status, extracted_at, loaded_at
            FROM meta._source_registry
            WHERE source_id = :source_id
            ORDER BY extracted_at DESC LIMIT :limit
        """)
        return pd.read_sql(query, self.engine, params={"source_id": source_id, "limit": limit})
   