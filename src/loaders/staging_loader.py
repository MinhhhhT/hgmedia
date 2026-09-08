"""
loaders/staging_loader.py
Load DataFrame thô (đã download từ MinIO) vào Postgres schema "staging".
KHÔNG áp business rule (join, tính cột phái sinh) — phần đó để dbt models xử lý.
Chỉ làm 3 việc: ép kiểu cơ bản, thêm cột metadata, ghi vào đúng bảng staging.<source_id>.
"""
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine

from src.connections import get_connection, get_sqlalchemy_uri


class StagingLoader:

    def __init__(self):
        conn_cfg = get_connection("dwh_postgres")
        self.engine = create_engine(get_sqlalchemy_uri(conn_cfg))

    def load(
        self, df: pd.DataFrame, staging_table: str, batch_id: str,
        load_mode: str = "append", upsert_key: list = None,
    ) -> int:
        """
        load_mode:
          - "truncate": xóa sạch bảng staging rồi insert (dùng cho Dim nhỏ, full reload mỗi lần)
          - "append": chỉ thêm dòng mới (dùng cho Fact incremental theo watermark)
          - "upsert": cần upsert_key, dùng khi muốn update record cũ + insert record mới
        staging_table dạng "staging.<source_id>" (vd staging.dim_partners)
        """
        if df is None or len(df) == 0:
            import logging
            logging.getLogger(__name__).warning(
                f"[{staging_table}] DataFrame rỗng (0 dòng) → KHÔNG load, giữ nguyên staging hiện tại"
            )
            return 0
        df = df.copy()
        # làm sạch tên cột: bỏ rỗng/xuống dòng, khử trùng tên
        new_cols, seen = [], {}
        for i, c in enumerate(df.columns):
            name = str(c).replace("\n", " ").strip() or f"col_{i}"
            if name in seen:
                seen[name] += 1; name = f"{name}_{seen[name]}"
            else:
                seen[name] = 0
            new_cols.append(name)
        df.columns = new_cols
        import json
        # ép cột chứa dict/list (jsonb Odoo) về chuỗi JSON để psycopg2 insert được
        for col in df.columns:
            if df[col].map(lambda v: isinstance(v, (dict, list))).any():
                df[col] = df[col].map(
                    lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                )
        df["_batch_id"] = batch_id
        df["_loaded_at"] = datetime.now()

        schema, table = staging_table.split(".")

        if load_mode == "truncate":
            with self.engine.begin() as conn:
                conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            df.to_sql(table, self.engine, schema=schema, if_exists="replace", index=False, chunksize=5000)

        elif load_mode == "upsert":
            if not upsert_key:
                raise ValueError("upsert_key bắt buộc khi load_mode='upsert'")
            self._upsert(df, schema, table, upsert_key)

        else:  # append (mặc định, dùng cho incremental)
            df.to_sql(table, self.engine, schema=schema, if_exists="append", index=False, chunksize=5000)

        return len(df)

    def _upsert(self, df: pd.DataFrame, schema: str, table: str, upsert_key: list):
        """Upsert đơn giản: load vào bảng tạm rồi merge bằng SQL (ON CONFLICT)."""
        temp_table = f"_tmp_{table}"
        df.to_sql(temp_table, self.engine, schema=schema, if_exists="replace", index=False)

        # tạo bảng đích nếu chưa có + unique index cho ON CONFLICT
        df.iloc[0:0].to_sql(table, self.engine, schema=schema, if_exists="append", index=False)
        key_clause = ", ".join(upsert_key)
        idx_name = f"uq_{table}_{'_'.join(upsert_key)}"
        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                f'CREATE UNIQUE INDEX IF NOT EXISTS "{idx_name}" '
                f"ON {schema}.{table} ({key_clause})"
            )

        cols = list(df.columns)
        non_key_cols = [c for c in cols if c not in upsert_key]
        update_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in non_key_cols])
        col_list = ", ".join(cols)
        merge_sql = f"""
            INSERT INTO {schema}.{table} ({col_list})
            SELECT {col_list} FROM {schema}.{temp_table}
            ON CONFLICT ({key_clause}) DO UPDATE SET {update_clause}
        """
        with self.engine.begin() as conn:
            conn.exec_driver_sql(merge_sql)
            conn.exec_driver_sql(f"DROP TABLE {schema}.{temp_table}")
            
    def ensure_table_exists(self, df: pd.DataFrame, staging_table: str):
        """Tạo bảng staging nếu chưa tồn tại, dựa theo dtype của DataFrame (dùng pandas to_sql tự suy kiểu)."""
        schema, table = staging_table.split(".")
        empty_df = df.iloc[0:0]
        empty_df.to_sql(table, self.engine, schema=schema, if_exists="append", index=False)
