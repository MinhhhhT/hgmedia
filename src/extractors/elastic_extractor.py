import logging, requests, pandas as pd, json, gc, io
from typing import Optional
from src.extractors.base import BaseExtractor, ExtractResult
log = logging.getLogger(__name__)

class ElasticExtractor(BaseExtractor):
    def __init__(self, source_config, connection_config):
        super().__init__(source_config)
        self.cc = connection_config

    def _auth(self):
        u, p = self.cc.get("user"), self.cc.get("password")
        return (u, p) if u else None

    def extract(self, watermark_filter=None):
        cfg = self.source_config
        index = cfg["es_index"]
        size = cfg.get("page_size", 5000)
        base = self.cc["host"].rstrip("/")
        url = f"{base}/{index}/_search"
        q = {"match_all": {}}
        if cfg.get("date_from"):
            q = {"range": {cfg.get("date_field", "Date"): {"gte": cfg["date_from"]}}}
        sf = cfg.get("sort_field", "_id")
        body = {"size": size, "query": q, "sort": [{sf: "asc"}, {"_doc": "asc"}]}
        auth = self._auth()

        stream = cfg.get("stream_to_staging")
        if stream:
            from sqlalchemy import create_engine
            from src.connections import get_connection, get_sqlalchemy_uri
            dwh = create_engine(get_sqlalchemy_uri(get_connection("dwh_postgres")))
            schema, table = cfg["target_staging_table"].split(".")
        else:
            from src.minio_client import MinIOClient
            minio = MinIOClient()
            batch_id = minio.make_batch_id(cfg["source_id"])
            minio_parts = []

        TIMESTAMP_SUFFIXES = ("At", "Date", "Time", "Utc", "UTC")
        CHUNK_PAGES = 10  # upload MinIO mỗi 10 page (50k dòng)

        def clean(df):
            for c in df.columns:
                if df[c].map(lambda v: isinstance(v, (dict, list))).any():
                    df[c] = df[c].map(
                        lambda v: json.dumps(v, ensure_ascii=False)
                        if isinstance(v, (dict, list)) else v
                    )
                if any(c.endswith(s) for s in TIMESTAMP_SUFFIXES):
                    df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")
            df["_source_id"] = cfg["source_id"]
            return df

        def get_dtype_map(df):
            from sqlalchemy import Text
            from sqlalchemy.dialects.postgresql import TIMESTAMP as PG_TS
            dtype = {}
            for c in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[c]):
                    dtype[c] = PG_TS(timezone=True)
                elif df[c].dtype == object:
                    dtype[c] = Text()
            return dtype

        rows, after, total, page, part = [], None, 0, 0, 0
        dtype_map = None

        while True:
            if after:
                body["search_after"] = after
            r = requests.post(url, json=body, auth=auth, timeout=120)
            r.raise_for_status()
            hits = r.json()["hits"]["hits"]
            if not hits:
                break
            page += 1
            batch = [{**h["_source"], "_es_id": h["_id"]} for h in hits]
            total += len(hits)
            after = hits[-1]["sort"]

            if stream:
                df = clean(pd.json_normalize(batch))
                if dtype_map is None:
                    dtype_map = get_dtype_map(df)
                df.to_sql(
                    table, dwh, schema=schema, index=False,
                    if_exists=("replace" if page == 1 else "append"),
                    method="multi", chunksize=1000,
                    dtype=dtype_map,
                )
                log.info(f"[{cfg['source_id']}] page {page}: +{len(hits)} (tổng {total}) -> staging")
                del df, batch, r
                gc.collect()
            else:
                rows += batch
                # upload MinIO mỗi CHUNK_PAGES page để tránh OOM
                if page % CHUNK_PAGES == 0:
                    part += 1
                    chunk_df = clean(pd.json_normalize(rows))
                    path = minio.upload_dataframe_part(chunk_df, cfg, batch_id, part)
                    minio_parts.append(path)
                    log.info(f"[{cfg['source_id']}] upload part {part}: {len(rows)} dòng -> MinIO")
                    rows = []
                    del chunk_df
                    gc.collect()

            if len(hits) < size:
                break

        # upload phần còn lại chưa đủ CHUNK_PAGES
        if not stream and rows:
            part += 1
            chunk_df = clean(pd.json_normalize(rows))
            path = minio.upload_dataframe_part(chunk_df, cfg, batch_id, part)
            minio_parts.append(path)
            log.info(f"[{cfg['source_id']}] upload part {part} (cuối): {len(rows)} dòng -> MinIO")
            del chunk_df
            gc.collect()

        if stream:
            return ExtractResult(
                dataframe=pd.DataFrame(), row_count=total,
                checksum=None, watermark_value=None,
                source_meta={"index": index, "streamed": True}
            )

        # trả về path part đầu tiên để load_task biết prefix
        first_path = minio_parts[0] if minio_parts else ""
        return ExtractResult(
            dataframe=pd.DataFrame(), row_count=total,
            checksum=None, watermark_value=None,
            source_meta={"index": index, "batch_id": batch_id,
                         "minio_path": first_path, "streamed": False}
        )

    def has_changed(self, last):
        return True