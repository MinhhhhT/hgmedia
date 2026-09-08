"""
minio_client.py
Wrapper mỏng quanh thư viện minio để upload/download raw data (parquet) vào lớp bronze.
"""
import io
from datetime import datetime
import gc
import pandas as pd
from minio import Minio
import logging
logger = logging.getLogger(__name__)
from src.connections import get_connection


class MinIOClient:

    def __init__(self):
        cfg = get_connection("minio")
        self.client = Minio(
            cfg["endpoint"],
            access_key=cfg["access_key"],
            secret_key=cfg["secret_key"],
            secure=cfg["secure"],
        )
        self.bucket = cfg["bucket_raw"]
        self._ensure_bucket()

    def _ensure_bucket(self):
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    def build_path(self, source_config: dict, batch_id: str) -> str:
        st = source_config["source_type"]
        if st == "sql":
            conn = source_config["connection"]
            schema = source_config.get("schema", "default")
            table = source_config["source_table"]
            raw_key = f"{conn}/{schema}.{table}"
        elif st == "google_sheet":
            key = source_config.get("spreadsheet_id") or source_config.get("folder_id")
            ws = source_config.get("worksheet_name") or "all"
            raw_key = f"google_sheet/{key}/{ws}"
        else:
            raw_key = f"{st}/{source_config['source_id']}"
        return f"raw/{raw_key}/{batch_id}/data.parquet"

    def upload_dataframe(self, df: pd.DataFrame, source_config: dict, batch_id: str) -> str:
        path = self.build_path(source_config, batch_id)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        self.client.put_object(
            self.bucket, path, buffer, length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream",
        )
        return f"s3://{self.bucket}/{path}"

    def upload_dataframe_part(self, df: pd.DataFrame, source_config: dict, batch_id: str, part: int) -> str:
        """Upload 1 phần data (part) lên MinIO theo dạng part-000x.parquet"""
        path = self.build_path(source_config, batch_id).replace("data.parquet", f"part-{part:04d}.parquet")
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        self.client.put_object(
            self.bucket, path, buffer, length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream",
        )
        return f"s3://{self.bucket}/{path}"

    def download_dataframe(self, minio_path: str) -> pd.DataFrame:
        """Hỗ trợ cả file đơn và multi-part (tự detect theo prefix)"""
        path = minio_path.replace(f"s3://{self.bucket}/", "")

        import re
        prefix = re.sub(r'(data\.parquet|part-\d+\.parquet)$', '', path)

        objects = list(self.client.list_objects(self.bucket, prefix=prefix))
        part_files = [o.object_name for o in objects if "part-" in o.object_name]

        if part_files:
            dfs = []
            for p in sorted(part_files):
                response = self.client.get_object(self.bucket, p)
                try:
                    dfs.append(pd.read_parquet(io.BytesIO(response.read())))
                finally:
                    response.close()
                    response.release_conn()
            return pd.concat(dfs, ignore_index=True)

        # Single file
        response = self.client.get_object(self.bucket, path)
        try:
            buffer = io.BytesIO(response.read())
            return pd.read_parquet(buffer)
        finally:
            response.close()
            response.release_conn()

    def download_and_load(self, minio_path: str, staging_table: str, loader, batch_id: str) -> int:
        """Load từng part file trực tiếp vào staging, không concat vào RAM"""
        import re
        path = minio_path.replace(f"s3://{self.bucket}/", "")
        prefix = re.sub(r'(data\.parquet|part-\d+\.parquet)$', '', path)

        objects = list(self.client.list_objects(self.bucket, prefix=prefix))
        part_files = sorted([o.object_name for o in objects if "part-" in o.object_name])

        if part_files:
            total = 0
            for i, p in enumerate(part_files):
                response = self.client.get_object(self.bucket, p)
                try:
                    df = pd.read_parquet(io.BytesIO(response.read()))
                finally:
                    response.close()
                    response.release_conn()
                mode = "truncate" if i == 0 else "append"
                loader.load(df, staging_table=staging_table, batch_id=batch_id, load_mode=mode)
                total += len(df)
                logger.info(f"Load part {i+1}/{len(part_files)}: {len(df)} dòng → {staging_table}")
                del df
                gc.collect()
            return total

        # Single file — kiểm tra tồn tại trước khi get
        try:
            self.client.stat_object(self.bucket, path)
        except Exception:
            logger.warning(f"Không tìm thấy file parquet: {path} → bỏ qua rollback")
            return 0

        response = self.client.get_object(self.bucket, path)
        try:
            df = pd.read_parquet(io.BytesIO(response.read()))
        finally:
            response.close()
            response.release_conn()
        return loader.load(df, staging_table=staging_table, batch_id=batch_id, load_mode="truncate")
    @staticmethod
    def make_batch_id(source_id: str) -> str:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{source_id}_{ts}"