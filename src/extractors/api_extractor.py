import logging, requests, pandas as pd, gc
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dateutil.relativedelta import relativedelta
from threading import Lock
from src.extractors.base import BaseExtractor, ExtractResult

log = logging.getLogger(__name__)


class ApiExtractor(BaseExtractor):

    def __init__(self, source_config: dict):
        super().__init__(source_config)

    def extract(self, watermark_filter=None) -> ExtractResult:
        cfg = self.source_config

        # Nếu có parallel_by: month → chạy song song
        if cfg.get("parallel_by") == "month":
            return self._extract_parallel_monthly()

        return self._extract_single(cfg.get("params", {}).copy())

    def _get_month_ranges(self):
        """Tạo danh sách (date_from, date_to) theo tháng từ date_from đến nay."""
        cfg = self.source_config
        date_from_str = cfg.get("date_from", "2025-06-01")
        start = datetime.strptime(date_from_str, "%Y-%m-%d").replace(day=1)
        end = datetime.now().replace(day=1) + relativedelta(months=1)

        ranges = []
        cur = start
        while cur < end:
            nxt = cur + relativedelta(months=1)
            ranges.append((cur.strftime("%Y-%m-%d"), nxt.strftime("%Y-%m-%d")))
            cur = nxt
        return ranges

    def _extract_parallel_monthly(self) -> ExtractResult:
        cfg = self.source_config
        ranges = self._get_month_ranges()
        from_param = cfg.get("api_date_from_param", "fromDate")
        to_param = cfg.get("api_date_to_param", "toDate")
        max_workers = cfg.get("parallel_workers", 3)

        # Khi không stream vào staging, mỗi page được ghi ngay thành một
        # Parquet part. Không giữ toàn bộ dữ liệu của các tháng trong RAM.
        from src.minio_client import MinIOClient
        minio = MinIOClient()
        batch_id = minio.make_batch_id(cfg["source_id"])
        part_lock = Lock()
        next_part = 0
        minio_parts = []

        # Tạo engine 1 lần duy nhất, pool_size đủ cho tất cả luồng
        from sqlalchemy import create_engine, text
        from src.connections import get_connection, get_sqlalchemy_uri
        dwh = create_engine(
            get_sqlalchemy_uri(get_connection("dwh_postgres")),
            pool_size=max_workers + 5,
            max_overflow=10,
            pool_timeout=60,
        )

        # Truncate nếu load_mode = replace
        if cfg.get("load_mode") == "replace":
            schema, table = cfg["target_staging_table"].split(".")
            with dwh.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {schema}.{table}"))
                conn.commit()
            log.info(f"[{cfg['source_id']}] TRUNCATE {schema}.{table} xong")

        log.info(f"[{cfg['source_id']}] Parallel {max_workers} luồng × {len(ranges)} tháng")
        total_rows = 0

        def upload_page(dataframe, month_label):
            nonlocal next_part

            # Các monthly worker chạy song song nên part number phải duy nhất.
            with part_lock:
                next_part += 1
                part = next_part

            path = minio.upload_dataframe_part(
                dataframe,
                cfg,
                batch_id,
                part,
            )

            with part_lock:
                minio_parts.append(path)

            log.info(
                "[%s] %s upload part %s: %s dòng -> MinIO",
                cfg["source_id"],
                month_label,
                part,
                len(dataframe),
            )
        def fetch_month(date_from, date_to):
            params = cfg.get("params", {}).copy()
            params[from_param] = date_from
            params[to_param] = date_to
            return self._extract_single(
                params,
                month_label=f"{date_from[:7]}",
                dwh=dwh,
                on_page=(
                    upload_page
                    if not cfg.get("stream_to_staging")
                    else None
                ),
            )

        errors = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch_month, date_from, date_to): (date_from, date_to)
                for date_from, date_to in ranges
            }
            for future in as_completed(futures):
                date_from, date_to = futures[future]
                try:
                    row_count = future.result()
                    total_rows += row_count
                    log.info(f"[{cfg['source_id']}] ✓ {date_from[:7]}: {row_count} dòng (tổng {total_rows})")
                except Exception as e:
                    log.error(f"[{cfg['source_id']}] ✗ {date_from[:7]}: {e}")
                    errors.append(f"{date_from[:7]}: {e}")

        dwh.dispose()  # đóng pool sau khi xong

        if errors:
            raise RuntimeError(
                f"[{cfg['source_id']}] Không thể extract "
                f"{len(errors)} tháng: {'; '.join(errors)}"
            )

        if not cfg.get("stream_to_staging"):
            # run_extract dùng minio_path đầu tiên để nhận ra đây là batch
            # multi-part; run_load sẽ load toàn bộ các part cùng prefix.
            first_path = minio_parts[0] if minio_parts else ""
            return ExtractResult(
                dataframe=pd.DataFrame(), row_count=total_rows,
                checksum=None, watermark_value=None,
                source_meta={
                    "parallel": True,
                    "months": len(ranges),
                    "streamed": False,
                    "multi_part": True,
                    "batch_id": batch_id,
                    "minio_path": first_path,
                },
            )

        return ExtractResult(
            dataframe=pd.DataFrame(), row_count=total_rows,
            checksum=None, watermark_value=None,
            source_meta={"parallel": True, "months": len(ranges), "streamed": True}
        )
    def _extract_single(
        self,
        params: dict,
        month_label: str = "",
        dwh=None,
        on_page=None,
    ) -> int:
        cfg = self.source_config
        url = cfg["api_url"]
        verify_ssl = cfg.get("verify_ssl", True)
        headers = cfg.get("headers", {})
        data_key = cfg.get("data_key")
        scroll_key = cfg.get("scroll_key")
        has_more_key = cfg.get("has_more_key")
        params = params.copy()

        page = 0
        month_rows = 0

        while True:
            r = requests.get(url, params=params, headers=headers,
                            verify=verify_ssl, timeout=120)
            r.raise_for_status()
            resp = r.json()

            if isinstance(resp, dict):
                data = resp[data_key] if data_key else list(resp.values())[0]
            else:
                data = resp

            if not data:
                break

            page += 1
            month_rows += len(data)
            log.info(f"[{cfg['source_id']}] {month_label} page {page}: +{len(data)} (tháng: {month_rows})")

            if on_page is not None:
                df = pd.json_normalize(data)
                df["_source_id"] = cfg["source_id"]
                on_page(df, month_label)
                del df, data, r
                gc.collect()
            elif cfg.get("stream_to_staging"):
                df = pd.json_normalize(data)
                df["_source_id"] = cfg["source_id"]
                self._stream_to_db(df, cfg, dwh=dwh)  # truyền dwh vào
                del df, data, r
                gc.collect()

            # Kiểm tra còn trang không
            has_more = False
            if has_more_key and isinstance(resp, dict):
                has_more = bool(resp.get(has_more_key, False))
            if not has_more:
                break

            if scroll_key and isinstance(resp, dict):
                scroll_val = resp.get(scroll_key)
                if scroll_val:
                    params[scroll_key] = scroll_val
                else:
                    break
            else:
                break

        return month_rows


    def _stream_to_db(self, df: pd.DataFrame, cfg: dict, dwh=None, replace: bool = False):
        from sqlalchemy import Text
        from sqlalchemy.dialects.postgresql import TIMESTAMP as PG_TS

        # Dùng engine truyền vào, không tạo mới
        if dwh is None:
            from sqlalchemy import create_engine
            from src.connections import get_connection, get_sqlalchemy_uri
            dwh = create_engine(get_sqlalchemy_uri(get_connection("dwh_postgres")))

        schema, table = cfg["target_staging_table"].split(".")

        dtype = {}
        for c in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[c]):
                dtype[c] = PG_TS(timezone=True)
            elif df[c].dtype == object:
                dtype[c] = Text()

        df.to_sql(
            table, dwh, schema=schema, index=False,
            if_exists="append",
            method=None,      # ← dùng executemany mặc định, không giới hạn param
            chunksize=1000,   # ← giữ 1000 dòng/batch cho nhanh
            dtype=dtype,
        )

    def has_changed(self, last) -> bool:
        return True
