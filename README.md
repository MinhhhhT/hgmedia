   # DWH Pipeline — Google Sheet + Database → MinIO → Postgres (staging) → dbt (silver/gold)

## Chạy toàn bộ bằng Docker Compose

Yêu cầu: Docker Desktop/Engine đang chạy và PostgreSQL đã được cài trên Windows.
PostgreSQL phải lắng nghe trên interface cho phép Docker kết nối, firewall phải mở
cổng 5432, và các user trong `.env` phải có quyền kết nối. Nếu database chưa tồn
tại, user cần quyền `CREATEDB` để `airflow-init` tự tạo database.

```bash
cd dwh-pipeline-mapping/dwh-pipeline

# Chỉ cần làm một lần: điền credential thật vào .env hiện có.
# Có thể tham khảo danh sách biến trong .env.example.

docker compose up -d --build
docker compose ps
```

Ở lần chạy đầu, Compose sẽ tự build image Airflow có dbt, ODBC Driver 17 và
các Python package; migrate metadata database; tạo tài khoản Airflow; tải
`dbt_utils`; khởi tạo schema trên PostgreSQL Windows; đồng thời tạo bucket MinIO
`raw-bronze`.

Các endpoint mặc định trên host:

- Airflow: http://localhost:8081
- MinIO API: http://localhost:9000
- MinIO Console: http://localhost:9003
- DWH và Airflow metadata: PostgreSQL trên Windows, mặc định cổng 5432

Các port Docker có thể đổi bằng `AIRFLOW_EXPOSE_PORT`, `MINIO_API_EXPOSE_PORT`
và `MINIO_CONSOLE_EXPOSE_PORT` trong `.env`.

```bash
# Theo dõi quá trình init/build và các service chính
docker compose logs -f airflow-init minio-init airflow-api-server airflow-scheduler

# Dừng nhưng giữ nguyên dữ liệu
docker compose down
```

Pipeline EL (Extract-Load) bằng Python cho 2 nhóm nguồn hiện tại (Google Sheet, Database),
kết hợp dbt để transform. **Elasticsearch tạm bỏ qua**, sẽ bổ sung sau theo cùng pattern
(`src/extractors/elasticsearch_extractor.py` + `config/elasticsearch_sources.yaml`).

## Kiến trúc

```
[Google Sheet]  [Odoo PG + 3 SQL Server]
       │                  │
       ▼                  ▼
   Task 1: EXTRACT (Python) — đọc thô, KHÔNG transform
       │
       ▼
     MinIO (bronze/<source_type>/<source_id>/<batch_id>/data.parquet)
       │           SourceRegistry (meta._source_registry) ghi batch_id, watermark/checksum
       ▼
   Task 2: LOAD (Python) — load thô vào Postgres schema "staging"
       │
       ▼
   dbt models — business rule (join, tính cột phái sinh) bằng SQL
       │
       ▼
   Postgres schema "silver" (Dim/Fact chuẩn DWH) → "gold" (mart/báo cáo)
```

Task 1 và Task 2 độc lập nhau (đúng kiểu Airflow): Task 2 chỉ cần `batch_id + minio_path`,
không cần connection nguồn gốc còn sống.

## Cài đặt dev local

```bash
pip install -r requirements.txt
docker compose up -d
# Postgres: localhost:5432 (dev/Inda1234, db=data_warehouse) — đã tự tạo schema staging/silver/gold/meta
# MinIO Console: http://localhost:9001 (minioadmin/minioadmin)
```

## Dùng CLI

```bash
# Chạy 1 source cụ thể
python main.py run --id dim_partners

# Bỏ qua check has_changed, luôn extract lại
python main.py run --id fact_distribution --force

# Chạy tất cả nguồn theo loại
python main.py run --type google_sheet
python main.py run --type sql

# Rollback 1 hoặc nhiều source về batch cũ theo ngày
python main.py rollback --date 2026-06-01 --ids fact_distribution
python main.py rollback --date 2026-06-01    # rollback tất cả source trong config

# Xem lịch sử extract/load
python main.py history --id dim_partners
```

## Chạy dbt

```bash
cd dwh_dbt
dbt deps                 # cài dbt_utils
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## Chạy qua Airflow

```bash
pip install -r requirements-airflow.txt
export AIRFLOW_HOME=~/airflow
airflow db init
```

Copy project vào server Airflow, symlink `src/dags/*.py` vào `$AIRFLOW_HOME/dags/`.

- `el_google_sheet_pipeline` — 6h sáng mỗi ngày, Dynamic Task Mapping qua `config/google_sheet_sources.yaml`.
- `el_database_pipeline` — mỗi 4 tiếng, Dynamic Task Mapping qua `config/db_sources.yaml`
  (gồm cả Odoo lẫn 3 SQL Server, chạy song song vì khác connection).
- `dbt_transform_pipeline` — chờ cả 2 DAG trên (`ExternalTaskSensor`) rồi chạy `dbt run` + `dbt test`.

## Thêm 1 nguồn mới

1. Thêm entry vào `config/google_sheet_sources.yaml` (nếu Google Sheet) hoặc
   `config/db_sources.yaml` (nếu Database) — đủ `source_id`, `target_staging_table`, `load_mode`.
2. Nếu là DB mới (connection mới): khai báo thêm trong `src/connections.py`.
3. `python main.py run --id <source_id>` để test ngay, không cần sửa code.
4. Viết model dbt tương ứng trong `dwh_dbt/models/silver/dim/` hoặc `.../fact/`,
   `ref()` tới các Dim cần thiết — dbt tự lo thứ tự build.
5. Khai báo bảng staging mới vào `dwh_dbt/models/staging/_sources.yml`.

## Khác biệt so với pipeline Excel cũ (MISA)

| | Cũ (Excel-only) | Mới |
|---|---|---|
| Loại nguồn | Chỉ Excel | Google Sheet + nhiều DB (Postgres/SQL Server) cùng lúc |
| Phát hiện thay đổi | MD5 file | MD5 (Google Sheet) hoặc watermark column (DB, incremental) |
| Business logic | `transformers/*.py` (pandas, 1 file/bảng) | dbt models (SQL), `ref()` tự suy dependency |
| Thứ tự load | `PRIORITY_FILE_IDS` (list phẳng, hard-code) | dbt DAG tự động qua `ref()` |
| SCD / lịch sử Dim | Chưa hỗ trợ | dbt snapshot (xem `dwh_dbt/snapshots/`, cần bổ sung khi có yêu cầu) |
| Data quality | Không có | dbt tests (`not_null`, `unique`, `relationships`) |

## Thứ tự build Dim/Fact (9 layer theo Data Dictionary)

Toàn bộ 43 model trong `dwh_dbt/models/silver/{dim,fact}/` đã được sinh sẵn (skeleton) với
`ref()` đúng theo 9 layer phụ thuộc đã phân tích từ Data Dictionary. **Không cần tự viết DAG
resolver/topological sort thủ công** — `dbt run` tự đọc `ref()` trong từng file `.sql` và build
đúng thứ tự (Dim layer 0 → ... → Fact layer 8), tự động song song hoá các model cùng layer
không phụ thuộc nhau.

3 model đã có business rule thật (làm mẫu): `dim_partners`, `dim_company`, `fact_distribution`.
40 model còn lại là **skeleton `select *` + comment `-- TODO`** đánh dấu chỗ cần điền:
1. Tên bảng staging thật (hiện đang giả định trùng tên model, cần đối chiếu lại `target_staging_table`
   trong `config/google_sheet_sources.yaml` / `config/db_sources.yaml`).
2. Điều kiện JOIN thật với các bảng `ref()` đã có sẵn dòng comment gợi ý.
3. Business rule riêng (unnest GUID, tính cột phái sinh...) theo đúng "Quy tắc" trong Data Dictionary.
4. Tên cột khóa chính thật trong `_silver_models.yml` (hiện đang giả định convention `<table>_id`,
   cần sửa lại cho khớp PK/FK thật, ví dụ `dim_partners.partner_id` đã đúng nhưng nhiều bảng khác
   có thể khác convention).

Build thử riêng 1 nhánh (model đó + mọi thứ nó phụ thuộc):
```bash
dbt run --select +fact_distribution
```
Build toàn bộ theo đúng thứ tự layer:
```bash
dbt run
```
Xem sơ đồ DAG trực quan sau khi đã có dữ liệu/docs:
```bash
dbt docs generate && dbt docs serve
```


## Test kết nối trước khi chạy pipeline thật

1. Copy `.env.example` thành `.env`, điền thông tin thật (host/port/db/user/password cho từng nguồn).
2. Cài thêm dependency nếu chưa có: `pip install python-dotenv`.
3. Với Google Sheet: tải file JSON service account từ Google Cloud Console, đặt vào
   `config/gsheet_service_account.json` (hoặc đường dẫn khác, sửa `GSHEET_CREDENTIALS_PATH` trong `.env`),
   rồi **share quyền View** Google Sheet đó cho email của service account (dạng `...@...iam.gserviceaccount.com`).
4. Chạy:
   ```bash
   python scripts/test_connections.py
   ```
   Script chỉ test connect (SELECT 1 / list bucket / đọc credential file), không extract/load dữ liệu thật.
   Kết quả in `[OK]` hoặc `[FAIL] <lý do>` cho từng nguồn — sửa theo đúng lỗi báo ra.

## TODO khi triển khai thật

- Điền connection thật vào `src/connections.py` (hiện toàn placeholder).
- Điền `spreadsheet_id` thật + tạo Google Service Account, đặt credential JSON tại
  `config/gsheet_service_account.json`, share quyền view cho service account email lên từng Sheet.
- Xác nhận watermark column cho các bảng DB hiện đang để `watermark_column: null` (full reload).
- Bổ sung ~30 entry còn lại trong `config/db_sources.yaml` (mẫu hiện chỉ có vài bảng tiêu biểu)
  và ~25 dbt model còn lại theo 8 layer phụ thuộc đã phân tích từ Data Dictionary.
- Quyết định SCD Type 1 hay Type 2 cho từng Dim, viết `dwh_dbt/snapshots/*.sql` nếu cần Type 2.
- Khi sẵn sàng thêm Elasticsearch: viết `ElasticsearchExtractor` (implement `BaseExtractor`),
  thêm `config/elasticsearch_sources.yaml`, đăng ký trong `extractors/factory.py`,
  tạo DAG `el_elasticsearch_dag.py` riêng (lịch dày hơn, ví dụ mỗi giờ).

## Thiết lập Retention Policy Minio 7 ngày
Bước 1 — Cài MinIO Client (`mc`)
Mục đích: cài command-line client để quản lý MinIO và lifecycle policy từ WSL.

```bash
curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o /tmp/mc
chmod +x /tmp/mc
sudo install /tmp/mc /usr/local/bin/mc

mc --version

Bước 2 — Lấy password MinIO runtime
sudo grep MINIO_ROOT_PASSWORD /etc/systemd/system/minio.service
Bước 3 — Tạo alias mc đến MinIO
read -s MINIO_PASSWORD
Nhập giá trị MINIO_ROOT_PASSWORD ở bước 2 rồi nhấn Enter. Password sẽ không hiển thị trên terminal.
Bước 4: Kết nối mc đến MinIO hiện tại
mc alias set dwh-minio http://localhost:9001 hgmedia "$MINIO_PASSWORD"
Bước 5: Set retention 7 ngày cho toàn bộ raw-bronze
mc ilm rule add dwh-minio/raw-bronze --expire-days 7
Bước 6: Xác nhận thành công 
mc ilm rule ls dwh-minio/raw-bronze
Bạn cần thấy rule có DAYS TO EXPIRE là 7
