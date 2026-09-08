"""
tasks/load_task.py
Task 2 (độc lập với Task 1): nhận batch_id + minio_path (qua XCom hoặc CLI),
download từ MinIO, load vào schema staging. KHÔNG cần file/kết nối nguồn gốc còn sống.
"""
import logging
import gc
import io

from src.loaders.staging_loader import StagingLoader
from src.minio_client import MinIOClient
from src.source_registry import SourceRegistry

logger = logging.getLogger(__name__)


def run_load(source_config: dict, batch_id: str, minio_path: str):
    source_id = source_config["source_id"]
    staging_table = source_config["target_staging_table"]
    load_mode = source_config.get("load_mode", "append")
    upsert_key = source_config.get("upsert_key")

    minio = MinIOClient()
    loader = StagingLoader()
    registry = SourceRegistry()

    try:
        # detect multi-part
        path = minio_path.replace(f"s3://{minio.bucket}/", "")
        prefix = "/".join(path.split("/")[:-1]) + "/"
        objects = list(minio.client.list_objects(minio.bucket, prefix=prefix))
        part_files = sorted([o.object_name for o in objects if "part-" in o.object_name])

        if part_files:
            # load từng part một, không merge vào RAM
            total = 0
            for i, p in enumerate(part_files):
                response = minio.client.get_object(minio.bucket, p)
                try:
                    import pandas as pd
                    df = pd.read_parquet(io.BytesIO(response.read()))
                finally:
                    response.close()
                    response.release_conn()

                row_count = loader.load(
                    df, staging_table=staging_table, batch_id=batch_id,
                    load_mode="truncate" if i == 0 else "append",
                    upsert_key=upsert_key,
                )
                total += row_count
                logger.info(f"[{source_id}] Load part {i+1}/{len(part_files)}: {row_count} dòng")
                del df
                gc.collect()

            registry.mark_loaded(batch_id)
            logger.info(f"[{source_id}] Load xong {total} dòng vào {staging_table}")
            return total

        else:
            # single file: giữ nguyên logic cũ
            df = minio.download_dataframe(minio_path)
            row_count = loader.load(
                df, staging_table=staging_table, batch_id=batch_id,
                load_mode=load_mode, upsert_key=upsert_key,
            )
            registry.mark_loaded(batch_id)
            logger.info(f"[{source_id}] Load xong {row_count} dòng vào {staging_table}")
            return row_count

    except Exception as e:
        registry.mark_failed(batch_id, str(e))
        logger.error(f"[{source_id}] Load thất bại: {e}")
        raise