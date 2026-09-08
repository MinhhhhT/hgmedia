"""
tasks/rollback_task.py
Rollback 1 source_id về batch 'loaded' gần nhất <= target_date.
Không cần file/connection gốc vì dữ liệu đã có sẵn trên MinIO từ lúc extract.
"""
import logging

from src.loaders.staging_loader import StagingLoader
from src.minio_client import MinIOClient
from src.source_registry import SourceRegistry

logger = logging.getLogger(__name__)


def run_rollback(source_config: dict, target_date: str = None):
    source_id = source_config["source_id"]
    staging_table = source_config["target_staging_table"]

    registry = SourceRegistry()
    minio = MinIOClient()
    loader = StagingLoader()

    batch = registry.find_batch_for_rollback(source_id, target_date)
    if not batch:
        if target_date:
            logger.warning(f"[{source_id}] Không tìm thấy batch có file MinIO <= {target_date}")
        else:
            logger.warning(f"[{source_id}] Không tìm thấy batch có file MinIO để rollback")
        return

    label = f"[target_date={target_date}]" if target_date else "[latest]"
    logger.info(
        f"[{source_id}] Rollback về batch {batch['batch_id']} "
        f"(extracted_at={batch['extracted_at']}, {batch['row_count']} dòng) {label}"
    )

    # Load từng part trực tiếp, không concat vào RAM
    total = minio.download_and_load(
        batch["minio_path"],
        staging_table=staging_table,
        loader=loader,
        batch_id=batch["batch_id"]
    )
    logger.info(f"[{source_id}] Rollback xong: {total} dòng → {staging_table}")