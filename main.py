"""
main.py — CLI entrypoint.
Dùng để test pipeline cục bộ trước khi gắn vào Airflow.

Ví dụ:
    python main.py run --id dim_partners
    python main.py run --id fact_distribution --force
    python main.py run --type google_sheet          # chạy tất cả nguồn Google Sheet
    python main.py run --type sql                   # chạy tất cả nguồn DB
    python main.py run --type elastic               # chạy tất cả nguồn Elasticsearch
    python main.py run --type csv                   # chạy tất cả nguồn CSV
    python main.py run --type fx                    # chạy tất cả nguồn tỷ giá
    python main.py run --type api                   # chạy tất cả nguồn API
    python main.py rollback --id fact_distribution --date 2026-06-01
    python main.py history --id dim_partners
"""
import argparse
import logging

from src.config_loader import get_source_by_id, load_all_sources
from src.tasks.extract_task import run_extract
from src.tasks.load_task import run_load
from src.tasks.rollback_task import run_rollback
from src.source_registry import SourceRegistry

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


SOURCE_TYPES = ["google_sheet", "sql", "elastic", "csv", "fx", "api"]


def run_one(source_config: dict, force: bool = False):
    extract_result = run_extract(source_config, force=force)
    if extract_result is None:
        logger.info(f"[{source_config['source_id']}] Skip (không có thay đổi hoặc 0 dòng).")
        return
    if extract_result.get("streamed"):
        logger.info(f"[{source_config['source_id']}] Đã stream {extract_result.get('row_count')} dòng vào staging.")
        return
    run_load(source_config, extract_result["batch_id"], extract_result["minio_path"])


def cmd_run(args):
    if getattr(args, "month", None):
        src = get_source_by_id(args.id); src["month_filter"] = args.month
        run_one(src, force=args.force)
        return
    if args.id:
        run_one(get_source_by_id(args.id), force=args.force)
        return

    sources = load_all_sources()
    if args.type:
        sources = [s for s in sources if s["source_type"] == args.type]

    failures = []
    for src in sources:
        try:
            run_one(src, force=args.force)
        except Exception as e:
            failures.append((src["source_id"], str(e)))
            logger.error(f"[{src['source_id']}] Lỗi: {e}")

    if failures:
        logger.error("Batch EL hoàn tất với %d source lỗi:", len(failures))
        for source_id, error in failures:
            logger.error(" - %s: %s", source_id, error)
        raise SystemExit(1)


def cmd_rollback(args):
    ids = args.ids.split(",") if args.ids else [s["source_id"] for s in load_all_sources()]
    for source_id in ids:
        run_rollback(get_source_by_id(source_id), target_date=args.date)


def cmd_history(args):
    registry = SourceRegistry()
    df = registry.history(args.id)
    print(df.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(description="DWH Extract-Load CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_run = subparsers.add_parser("run", help="Chạy extract + load cho 1 hoặc nhiều source")
    p_run.add_argument("--id", help="source_id cụ thể")
    p_run.add_argument("--type", choices=SOURCE_TYPES, help="chạy tất cả nguồn theo loại")
    p_run.add_argument("--force", action="store_true", help="bỏ qua check has_changed, luôn extract lại")
    p_run.add_argument("--month", help="lọc theo tháng YYYY-MM (chỉ nạp tháng đó)")
    p_run.set_defaults(func=cmd_run)

    p_rollback = subparsers.add_parser("rollback", help="Rollback về batch cũ theo ngày")
    p_rollback.add_argument("--date", required=False, default=None, help="YYYY-MM-DD, để trống = rollback về batch gần nhất có file MinIO")
    p_rollback.add_argument("--ids", help="comma-separated source_id, để trống = tất cả")
    p_rollback.set_defaults(func=cmd_rollback)

    p_history = subparsers.add_parser("history", help="Xem lịch sử extract/load của 1 source")
    p_history.add_argument("--id", required=True)
    p_history.set_defaults(func=cmd_history)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
