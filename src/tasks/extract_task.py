"""
tasks/extract_task.py
Task 1 (độc lập với Task 2): đọc dữ liệu thô từ nguồn (Google Sheet / SQL),
upload lên MinIO, ghi vào SourceRegistry. Tự skip nếu nguồn chưa đổi.

Trả về dict {batch_id, minio_path, source_id} để Task 2 dùng qua XCom.
Nếu skip (không đổi) -> trả về None.
"""
import logging

from src.extractors.factory import get_extractor
from src.minio_client import MinIOClient
from src.source_registry import SourceRegistry

logger = logging.getLogger(__name__)


def run_extract(source_config: dict, force: bool = False) -> dict | None:
    source_id = source_config["source_id"]
    source_type = source_config["source_type"]
    connection_name = source_config.get("connection")

    registry = SourceRegistry()
    minio = MinIOClient()
    extractor = get_extractor(source_config)

    last_success = registry.get_last_success(source_id)
    last_marker = None
    if last_success:
        last_marker = last_success.get("watermark_value") or last_success.get("checksum")

    if not force and not extractor.has_changed(last_marker):
        logger.info(f"[{source_id}] Không có thay đổi, skip extract.")
        return None

    extra_kwargs = {}
    if source_type == "sql" and source_config.get("incremental") and last_marker:
        extra_kwargs["watermark_filter"] = last_marker

    result = extractor.extract(**extra_kwargs)

    # stream thẳng vào staging (không qua MinIO)
    if result.source_meta and result.source_meta.get("streamed"):
        registry.register_extracted(
            source_id=source_id, source_type=source_type, connection_name=connection_name,
            batch_id=minio.make_batch_id(source_id), minio_path="(streamed)",
            row_count=result.row_count, checksum=result.checksum,
            watermark_value=result.watermark_value,
        )
        logger.info(f"[{source_id}] Stream thẳng staging: {result.row_count} dòng.")
        return {"source_id": source_id, "streamed": True, "row_count": result.row_count}

    if result.row_count == 0:
        logger.info(f"[{source_id}] Extract ra 0 dòng, skip load.")
        return None

    # extractor đã tự upload parts lên MinIO (elastic multi-part)
    if result.source_meta and result.source_meta.get("minio_path"):
        batch_id = result.source_meta["batch_id"]
        minio_path = result.source_meta["minio_path"]
        registry.register_extracted(
            source_id=source_id, source_type=source_type, connection_name=connection_name,
            batch_id=batch_id, minio_path=minio_path, row_count=result.row_count,
            checksum=result.checksum, watermark_value=result.watermark_value,
        )
        logger.info(f"[{source_id}] Extract xong: {result.row_count} dòng -> {minio_path}")
        return {"source_id": source_id, "batch_id": batch_id, "minio_path": minio_path}

    # upload bình thường (single file)
    # Nếu extractor đã tự upload MinIO (multi_part=True)
    if result.source_meta and result.source_meta.get("multi_part"):
        batch_id = result.source_meta["batch_id"]
        minio_path = result.source_meta["minio_path"]
    else:
        # Upload thông thường (DataFrame nhỏ, 1 file)
        if result.row_count == 0:
            log.info(f"[{source_id}] Extract ra 0 dòng, skip load.")
            return None
        batch_id = minio.make_batch_id(source_id)
        minio_path = minio.upload_dataframe(result.dataframe, source_config, batch_id)

    registry.register_extracted(
        source_id=source_id, source_type=source_type, connection_name=connection_name,
        batch_id=batch_id, minio_path=minio_path, row_count=result.row_count,
        checksum=result.checksum, watermark_value=result.watermark_value,
    )
    logger.info(f"[{source_id}] Extract xong: {result.row_count} dòng -> {minio_path}")
    return {
    "source_id": source_id,
    "batch_id": batch_id,
    "minio_path": minio_path,
    "row_count": result.row_count,
    "multi_part": True
}
