# KẾ HOẠCH & HƯỚNG DẪN HOÀN THÀNH LAB 17: DATA PIPELINE ENGINEERING
**Hệ thống:** Data Pipeline cho Nền tảng AI Hỗ trợ Khách hàng (AICB-P2T2)  
**Mục tiêu điểm số:** 100/100 điểm chính + 10/10 điểm thưởng (Tổng tối đa 110/100)

---

## I. TỔNG QUAN BÀI LAB & KIẾN TRÚC HỆ THỐNG

### 1. Luồng dữ liệu (Medallion Architecture)
```
Postgres tickets (CDC)  ─┐
S3 transcripts (JSON)   ─┼─→  Bronze  ─→  Silver  ─┬─→  gold_doc_chunks    (RAG index - Đối chứng)
Kafka events + feedback ─┘                          ├─→  gold_training_set  (Classifier - Nhiệm vụ 1)
                                                    └─→  gold_feature_daily (Routing agent - Nhiệm vụ 2)
                                              (Quarantine) →  quarantine_tickets  (Nhiệm vụ 3)
```

### 2. Công nghệ sử dụng
- **Data Warehouse:** DuckDB (`warehouse.duckdb`).
- **Data Transformation:** dbt (`dbt-duckdb`).
- **Orchestration:** Airflow DAG (`dags/ai_training_pipeline.py` - phân tích cấu hình).
- **Storage / Ingestion:** Parquet, JSONL, Commit log consumer.

### 3. Bộ tiêu chuẩn đánh giá và chỉ số mục tiêu (`expected/`)
| Bảng / Hạng mục | Trạng thái ổn định (3 lượt) | Số hàng kỳ vọng | Ghi chú & Yêu cầu cốt lõi |
|---|:---:|:---:|---|
| `gold_training_set` | ✓ OK (Checksum đồng nhất) | **12.480** | Idempotent, 1 row / 1 ticket (không lặp) |
| `gold_feature_daily` | ✓ OK (Checksum đồng nhất) | **9.100** | Xử lý late data (14 ngày × 650 customer) |
| `gold_doc_chunks` | ✓ OK (Checksum đồng nhất) | **31.200** | Nhóm đối chứng (không được làm ảnh hưởng) |
| `quarantine_tickets`| ✓ OK (Checksum đồng nhất) | **312** | 1 row / 1 bản ghi CDC lỗi (không quarantine nhầm) |
| `silver_tickets` | — | **12.480** | Đủ ticket, `priority` ∈ 1..4, không NULL |
| `dbt test` | — | **> 9 tests pass** | Enforce Contract `true` + test accepted values |
| DAG Parameters | — | `catchup=False`, `max_active_runs=1` | Chống chạy chồng lấn |

---

## II. QUY TRÌNH CHUẨN BỊ & MÔI TRƯỜNG LÀM VIỆC

### 1. Khởi tạo môi trường
Trong terminal (Linux / macOS / Git Bash / WSL trên Windows):
```bash
# 1. Tạo venv, cài dependencies, sinh dữ liệu seed ban đầu
make setup

# 2. Chạy thử pipeline 1 lượt
make pipeline

# 3. Chạy kiểm tra 3 lượt và xem bảng đánh giá ban đầu
make verify
```

> **Lệnh thao tác nhanh trong quá trình làm:**
> - `make quick`: Chạy 1 lượt (kiểm tra nhanh số hàng và lỗi cú pháp dbt).
> - `make verify`: Chạy 3 lượt (bắt buộc dùng để xác nhận tính ổn định và checksum).
> - `make reset`: Xoá file DB `warehouse.duckdb` để chạy lại từ đầu.
> - `make clean`: Dọn dẹp sạch sẽ trước khi nộp bài.

### 2. Thiết lập hàm truy vấn nhanh DuckDB (Query Helper)
Định nghĩa hàm query trong Terminal để điều tra dữ liệu trực tiếp:
```bash
q() { .venv/bin/python -c "
import duckdb, sys
duckdb.connect('warehouse.duckdb').sql(sys.argv[1]).show(max_rows=40)
" "$1"; }

# Ví dụ kiểm tra:
q "select count(*) from gold_training_set"
```

---

## III. KẾ HOẠCH CHI TIẾT TỪNG NHIỆM VỤ

---

### NHIỆM VỤ 1: Khắc phục lỗi phình dữ liệu bảng Training (`gold_training_set`)
*Thời lượng dự kiến: 25 - 30 phút | Thang điểm: 27 điểm (12đ ổn định + 12đ số hàng + 3đ dedup)*

#### 1. Điều tra & Thu thập số liệu
1. Chạy `make reset && make pipeline` rồi kiểm tra số lượng hàng trong `gold_training_set`. Chạy `make pipeline` lần thứ 2 và đếm lại.
2. Query tìm các ticket bị trùng lặp:
   ```sql
   select ticket_id, count(*) as n
   from gold_training_set
   group by 1 having n > 1
   order by n desc limit 10;
   ```
3. Kiểm tra nguồn CDC:
   ```sql
   select op, count(*) from bronze_tickets_cdc group by 1 order by 1;
   ```
   *Nhận xét:* Nguồn CDC có `op = 'u'` (update). Một ticket được tạo ngày D1 và sửa ngày D2 sẽ rơi vào 2 partition ngày khác nhau.

#### 2. Phân tích nguyên nhân gốc rễ (Root Cause)
- Model `gold_training_set.sql` được cấu hình `materialized = 'incremental'` nhưng **thiếu `unique_key` và `incremental_strategy`**.
- Mặc định, dbt sẽ thực hiện chiến lược `append` (câu lệnh `INSERT INTO target SELECT ...`). Khi chạy lại một ngày hoặc khi một ticket có nhiều bản ghi cập nhật ở các ngày khác nhau, dbt không ghi đè mà chèn thêm bản ghi mới, dẫn đến trùng lặp `ticket_id` và tăng số lượng hàng sau mỗi lượt chạy.
- Về phía Airflow DAG: `catchup=True` và `max_active_runs` chưa giới hạn khiến khi Clear Task, Airflow kích hoạt nhiều DAG run đồng thời/chạy bù dồn dập, làm trầm trọng thêm việc ghi đè/chèn trùng lặp.

#### 3. Các bước chỉnh sửa
- **Bước 1:** Chỉnh sửa file [gold_training_set.sql](file:///d:/AITHUCCHIEN/Labs/Day17-Track2-HaHoangTuanHung-2A202601629/dbt/models/gold/gold_training_set.sql):
  Khai báo `unique_key` và `incremental_strategy` trong khối `config()`:
  ```sql
  {{ config(
      materialized         = 'incremental',
      unique_key           = 'ticket_id',
      incremental_strategy = 'merge',
      on_schema_change     = 'fail'
  ) }}
  ```
  *(Giữ nguyên khối `where _ingested_at >= ...` để tối ưu quét theo partition).*
- **Bước 2:** Chỉnh sửa file [ai_training_pipeline.py](file:///d:/AITHUCCHIEN/Labs/Day17-Track2-HaHoangTuanHung-2A202601629/dags/ai_training_pipeline.py):
  Cập nhật tham số DAG:
  ```python
  catchup=False,
  max_active_runs=1,
  ```

#### 4. Tiêu chí kiểm chứng
- Chạy `make verify`.
- `gold_training_set`: Đạt `ỔN ĐỊNH ✓`, Số hàng đúng **12.480**.
- Dòng `gold_training_set: 1 hàng / 1 ticket`: `✓ không lặp`.
- Dòng `DAG: catchup / max_active_runs`: `✓ False / 1`.

---

### NHIỆM VỤ 2: Xử lý dữ liệu về muộn (Late-arriving data) cho `gold_feature_daily`
*Thời lượng dự kiến: 30 - 35 phút | Thang điểm: 26 điểm (12đ ổn định + 12đ số hàng + 2đ P99)*

#### 1. Điều tra & Đo lường phân bố độ trễ (P99)
Chạy truy vấn đo độ trễ từ `bronze_events`:
```sql
select
    quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.50) as p50_ngay,
    quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.95) as p95_ngay,
    quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.99) as p99_ngay,
    max(date_diff('second', event_time, _ingested_at)/86400.0)                 as max_ngay,
    avg(case when _ingested_at > event_time + interval 1 day then 1.0 else 0 end) as ty_le_late
from bronze_events;
```
> **Lưu ý:** Ghi lại chính xác giá trị **P99** (khoảng ~3 ngày) vào báo cáo.

Kiểm tra các cặp `(event_date, customer_id)` bị thiếu:
```sql
select s.event_date, count(distinct s.customer_id) as so_cap_thieu
from silver_events s
left join gold_feature_daily g
  on g.event_date = s.event_date and g.customer_id = s.customer_id
where g.customer_id is null
group by 1 order by 1;
```

#### 2. Phân tích nguyên nhân gốc rễ (Root Cause)
- Điều kiện incremental hiện tại: `where event_date > (select max(event_date) from {{ this }})`.
- Cơ chế lọc này chỉ lấy dữ liệu có ngày sự kiện hoàn toàn mới hơn ngày lớn nhất từng thấy trong bảng target. Khi có các sự kiện phát sinh ở ngày cũ (ví dụ `event_date = 08-12`) nhưng truyền về kho muộn (`_ingested_at = 08-15`), `max(event_date)` lúc này đã là `08-14` hoặc `08-15`. Do đó, điều kiện `event_date > max(event_date)` đã đánh rơi toàn bộ các sự kiện về muộn này.
- Khi nới rộng khoảng thời gian xử lý (lookback window), cùng một cặp `(event_date, customer_id)` sẽ được tính toán lại qua nhiều batch. Nếu không có `unique_key`, dữ liệu sẽ bị cộng dồn trùng lặp.

#### 3. Các bước chỉnh sửa
Chỉnh sửa file [gold_feature_daily.sql](file:///d:/AITHUCCHIEN/Labs/Day17-Track2-HaHoangTuanHung-2A202601629/dbt/models/gold/gold_feature_daily.sql):
- Khai báo khoá phức hợp (`composite unique_key`) và chiến lược `merge` / `delete+insert`.
- Cập nhật điều kiện lọc lùi lại theo cửa sổ lookback (dựa trên P99 = 3 ngày):
```sql
{{ config(
    materialized         = 'incremental',
    unique_key           = ['event_date', 'customer_id'],
    incremental_strategy = 'merge',
    on_schema_change     = 'fail'
) }}

select
    event_date,
    customer_id,
    customer_name,
    segment,
    count(*)                                                  as n_events,
    count(distinct ticket_id)                                 as n_tickets,
    sum(case when is_escalated then 1 else 0 end)             as n_escalated,
    round(avg(latency_ms), 2)                                 as avg_latency_ms,
    quantile_cont(latency_ms, 0.95)::int                      as p95_latency_ms,
    sum(tokens_in)                                            as tokens_in,
    sum(tokens_out)                                           as tokens_out
from {{ ref('silver_events') }}

{% if is_incremental() %}
where event_date >= (select max(event_date) from {{ this }}) - interval 3 day
{% endif %}

group by 1, 2, 3, 4
```

#### 4. Tiêu chí kiểm chứng
- Chạy `make verify`.
- `gold_feature_daily`: Đạt `ỔN ĐỊNH ✓`, Số hàng đúng **9.100** (khớp 14 ngày × 650 customer).
- `gold_training_set` và `gold_doc_chunks` vẫn giữ nguyên kết quả chuẩn.

---

### NHIỆM VỤ 3: Chuẩn hoá Schema Evolution & Thiết lập Data Contract / Quarantine
*Thời lượng dự kiến: 30 - 35 phút | Thang điểm: 20 điểm*

#### 1. Điều tra & Phân loại dữ liệu `priority_raw`
Kiểm tra phân bố các giá trị trong Bronze CDC:
```sql
select priority_raw, count(*) from bronze_tickets_cdc group by 1 order by 2 desc;
```
Phân loại chính xác 3 nhóm:
1. **Nhóm 1 (Số hợp lệ):** `'1'`, `'2'`, `'3'`, `'4'` $\rightarrow$ Giữ nguyên kiểu integer.
2. **Nhóm 2 (Schema evolution - nhãn chuỗi hợp lệ):** `'urgent'` $\rightarrow$ 1, `'high'` $\rightarrow$ 2, `'medium'` $\rightarrow$ 3, `'low'` $\rightarrow$ 4.
3. **Nhóm 3 (Dữ liệu lỗi thật sự):** `'P1'`, `'unknown'`, `'0'`, `'5'`, `'-1'`, `''`, `NULL` $\rightarrow$ Trả về `NULL` để đưa vào bảng cách ly `quarantine_tickets`.

#### 2. Phân tích nguyên nhân gốc rễ (Root Cause)
- Khi backend đổi kiểu `priority` sang chuỗi chữ, hàm `try_cast(priority_raw as integer)` cũ bị lỗi 2 chiều:
  + Biến các nhãn chuỗi hợp lệ (`urgent`, `high`,...) thành `NULL` (mất dữ liệu sạch).
  + Chấp nhận các số ngoài contract như `0`, `5`, `-1` vì chúng ép kiểu integer thành công.
- Ràng buộc contract chưa được kích hoạt (`enforced: false`), thiếu bài test miền giá trị trong dbt schema.
- Trong `silver_tickets.sql`, nếu xếp hạng `row_number()` trước khi lọc bỏ bản ghi hỏng thì một ticket có bản ghi mới nhất bị lỗi sẽ làm mất luôn cả ticket đó khỏi tầng Silver.

#### 3. Các bước chỉnh sửa (4 file)

- **File 1: [normalize_priority.sql](file:///d:/AITHUCCHIEN/Labs/Day17-Track2-HaHoangTuanHung-2A202601629/dbt/macros/normalize_priority.sql)**
  Thay thế logic ép kiểu bằng `CASE WHEN`:
  ```sql
  {% macro normalize_priority(col) %}
      case
          when {{ col }} in ('1', '2', '3', '4') then cast({{ col }} as integer)
          when lower(trim({{ col }})) = 'urgent' then 1
          when lower(trim({{ col }})) = 'high'   then 2
          when lower(trim({{ col }})) = 'medium' then 3
          when lower(trim({{ col }})) = 'low'    then 4
          else null
      end
  {% endmacro %}

  {% macro priority_reject_reason(col) %}
      case
          when {{ col }} is null or trim({{ col }}) = '' then 'priority bị rỗng hoặc NULL'
          when try_cast({{ col }} as integer) is not null and cast({{ col }} as integer) not between 1 and 4 
              then 'priority là số nằm ngoài khoảng 1..4'
          else 'priority chuỗi không hợp lệ (không thuộc urgent/high/medium/low)'
      end
  {% endmacro %}
  ```

- **File 2: [silver_tickets.sql](file:///d:/AITHUCCHIEN/Labs/Day17-Track2-HaHoangTuanHung-2A202601629/dbt/models/silver/silver_tickets.sql)**
  Lọc bỏ bản ghi lỗi **trước** khi đánh số thứ tự `row_number()`:
  ```sql
  {{ config(materialized = 'table') }}

  with valid_cdc as (

      select
          *,
          {{ normalize_priority('priority_raw') }} as priority_clean
      from {{ source('bronze', 'bronze_tickets_cdc') }}
      where {{ normalize_priority('priority_raw') }} is not null

  ),

  ranked as (

      select
          *,
          row_number() over (
              partition by ticket_id
              order by event_time desc, cdc_seq desc
          ) as _rn
      from valid_cdc

  ),

  latest as (select * from ranked where _rn = 1)

  select
      ticket_id,
      customer_id,
      customer_name,
      segment,
      priority_clean as priority,
      category,
      channel,
      status,
      csat,
      first_response_sec,
      subject,
      body,
      event_time as updated_at,
      _ingested_at
  from latest
  where op <> 'd'
  ```

- **File 3: [quarantine_tickets.sql](file:///d:/AITHUCCHIEN/Labs/Day17-Track2-HaHoangTuanHung-2A202601629/dbt/models/silver/quarantine_tickets.sql)**
  Đổi điều kiện lọc `where false` thành lấy các bản ghi có priority không chuẩn hoá được:
  ```sql
  where {{ normalize_priority('priority_raw') }} is null
  ```

- **File 4: [schema.yml](file:///d:/AITHUCCHIEN/Labs/Day17-Track2-HaHoangTuanHung-2A202601629/dbt/models/silver/schema.yml)**
  Bật enforce contract và kích hoạt tests cho cột `priority`:
  ```yaml
      config:
        contract:
          enforced: true

      columns:
        ...
        - name: priority
          data_type: integer
          description: "1 = urgent … 4 = low. Contract với team backend."
          tests:
            - not_null
            - accepted_values:
                values: [1, 2, 3, 4]
                quote: false
  ```

#### 4. Tiêu chí kiểm chứng
- Chạy `make verify`.
- `dbt test`: pass toàn bộ (tối thiểu 11 tests).
- `silver_tickets.priority ∈ 1..4, không NULL`: `✓ sạch`.
- `quarantine_tickets đúng số bản ghi lỗi`: `✓ 312 / 312`.
- `silver_tickets`: Đủ **12.480** tickets.

---

### BÀI TẬP MỞ RỘNG (EXTRA) — THƯỞNG +10 ĐIỂM

---

### BÀI A: Tối ưu hoá truy vấn Dashboard chậm (Small-File Problem & Partitioning)
*Thưởng: +5 điểm | Thời lượng: 20 phút*

#### 1. Đo lường hiện trạng
Chạy `make seed-extra` rồi đo baseline:
```bash
make seed-extra
make explain
make plan
```
Ghi lại: `rows scanned` (rất cao do DuckDB đọc 5.000 file parquet nhỏ), số files (5.000 files).

#### 2. Phân tích nguyên nhân & Hướng giải quyết
- **Nguyên nhân:** 
  1. *Small-file problem:* 5.000 file dung lượng nhỏ, engine tốn chi phí mở file và đọc metadata.
  2. *Thiếu cấu trúc Partition theo đường dẫn:* Thư mục không phân chia theo ngày, engine buộc phải scan toàn bộ 5.000 files.
  3. *Điều kiện filter Non-Sargable:* Trong `dashboard.sql` dùng `strftime(event_time, '%Y-%m-%d') = '2026-08-09'`, hàm bọc quanh cột khiến DuckDB không dùng được min/max statistics của Parquet row groups để prune.
- **Cách khắc phục:**
  - Viết lại [tools/compact.py](file:///d:/AITHUCCHIEN/Labs/Day17-Track2-HaHoangTuanHung-2A202601629/tools/compact.py): Đọc toàn bộ file cũ, sắp xếp `ORDER BY customer_name, event_time`, xuất ra thư mục `data/gold_events_v2` với `PARTITION_BY (event_date)` và `ROW_GROUP_SIZE 100000` (hoặc tương đương).
  - Viết lại [queries/dashboard.sql](file:///d:/AITHUCCHIEN/Labs/Day17-Track2-HaHoangTuanHung-2A202601629/queries/dashboard.sql): Trỏ vào `read_parquet('data/gold_events_v2/*/*.parquet', hive_partitioning=1)`, viết lại điều kiện thời gian dạng sargable:
    ```sql
    where customer_name = 'ACME'
      and event_date = '2026-08-09'
    ```
- **Kiểm chứng:**
  Chạy `make compact && make explain`. `rows scanned` giảm $\ge 10\times$, `result hash` giữ nguyên.

---

### BÀI B: Consumer Crash Recovery & Idempotent Ingestion (Delivery Semantics)
*Thưởng: +5 điểm | Thời lượng: 20 phút*

#### 1. Hiện tượng & Phân tích cơ chế
- Hiện tại trong [ingest/consumer.py](file:///d:/AITHUCCHIEN/Labs/Day17-Track2-HaHoangTuanHung-2A202601629/ingest/consumer.py): Thứ tự đang là `consumer.commit()` $\rightarrow$ `maybe_crash()` $\rightarrow$ `write_batch()`.
- Đây là ngữ nghĩa **At-most-once**: commit offset trước khi ghi. Nếu tiến trình bị crash ở `maybe_crash()`, dữ liệu batch đó chưa kịp ghi nhưng offset đã tăng $\rightarrow$ Mất dữ liệu khi restart.
- Nếu đảo thứ tự: `write_batch()` $\rightarrow$ `maybe_crash()` $\rightarrow$ `consumer.commit()`: Ngữ nghĩa chuyển thành **At-least-once**. Nếu crash sau khi ghi nhưng trước khi commit offset, khi restart consumer sẽ đọc lại batch đó. Nếu dùng `INSERT` thuần, dữ liệu sẽ bị nhân đôi (duplicate).

#### 2. Cách khắc phục
- Đổi DDL bảng `bronze_events_stream` để có `PRIMARY KEY (event_id)`.
- Chuyển lệnh ghi trong `write_batch` thành Idempotent Insert:
  ```sql
  insert into bronze_events_stream values (?, ?, ?, ?, ?, ?, ?, ?)
  on conflict (event_id) do update set
      ticket_id = excluded.ticket_id,
      customer_id = excluded.customer_id,
      customer_name = excluded.customer_name,
      event_type = excluded.event_type,
      latency_ms = excluded.latency_ms,
      event_time = excluded.event_time,
      _ingested_at = excluded._ingested_at
  ```
  *(Hoặc `ON CONFLICT (event_id) DO NOTHING` tuỳ chiến lược cập nhật)*.
- Trong `consume()`: Đổi thứ tự thành `write_batch(con, batch)` $\rightarrow$ `consumer.commit()` $\rightarrow$ `maybe_crash()`.

#### 3. Tiêu chí kiểm chứng
- Chạy `make crash-test` $\rightarrow$ Thông báo `NHIỆM VỤ MỞ RỘNG B: ĐẠT`.
- Chạy `make verify` đảm bảo toàn bộ pipeline chính không bị ảnh hưởng.

---

## IV. HƯỚNG DẪN VIẾT BÁO CÁO (`REPORT_TEMPLATE.md`)
Điểm phần báo cáo (20 điểm) đòi hỏi giải thích đúng **cơ chế (Root Cause)** chứ không chỉ mô tả cách sửa code:

1. **Mục 1 (Idempotent):**
   - *Triệu chứng:* Sau mỗi lần chạy pipeline hoặc Clear Task, số hàng `gold_training_set` tăng gấp bội.
   - *Root Cause:* Incremental model thiếu `unique_key`, dbt mặc định dùng lệnh `INSERT` thay vì `MERGE`. Source CDC có các bản ghi update (`op='u'`) làm cùng một `ticket_id` xuất hiện ở nhiều ngày khác nhau và bị chèn trùng lặp.
2. **Mục 2 (Late-arriving data):**
   - *Triệu chứng:* `gold_feature_daily` thiếu ~5% số hàng ở các ngày quá khứ.
   - *P99 đo được:* Ghi chính xác giá trị tính toán (ví dụ: ~3.0 ngày).
   - *Root Cause:* Mệnh đề `where event_date > (select max(event_date) from {{ this }})` chỉ lọc các event có ngày phát sinh lớn hơn ngày lớn nhất trong kho, làm mất các event phát sinh ngày cũ nhưng gửi về trễ.
   - *Trả lời câu hỏi thiết kế:* Chọn P99 thay vì max vì max có thể bị ảnh hưởng bởi dữ liệu rác/outliers (ví dụ event lệch hàng năm), khiến mỗi lần chạy phải scan và merge lại toàn bộ lịch sử, tốn chi phí tài nguyên không cần thiết.
3. **Mục 3 (Schema evolution & Contract):**
   - *Triệu chứng:* Model dự đoán giảm độ chính xác từ 08-10, `priority` trong Silver chứa nhiều NULL và giá trị ngoài 1..4.
   - *Root Cause:* Nguồn đổi format từ số sang nhãn chữ; `try_cast` biến nhãn hợp lệ thành NULL và cho phép số ngoài 1..4 đi qua; chưa enforce contract; thứ tự dedup lọc sai làm mất ticket.
   - *Trả lời câu hỏi thiết kế:* Nên chặn/lọc dữ liệu sai ở tầng Silver và đẩy vào Quarantine thay vì chặn ở Bronze hoặc làm fail pipeline, vì:
     + Tầng Bronze cần lưu trữ nguyên vẹn dữ liệu thô (Raw/Immutable) để phục vụ audit và re-process khi cần.
     + Một số lượng nhỏ bản ghi lỗi không nên làm gián đoạn toàn bộ luồng xử lý của hàng trăm nghìn bản ghi hợp lệ khác phục vụ người dùng.

---

## V. CHECKLIST TRƯỚC KHI NỘP BÀI

- [ ] Chạy `make verify` $\rightarrow$ Đạt **4/4 tiêu chí**, tất cả các bảng đều `ỔN ĐỊNH ✓` và đúng số hàng.
- [ ] Chạy `make explain` (nếu làm bài A) $\rightarrow$ Rows scanned giảm $\ge 10\times$, Hash trùng khớp.
- [ ] Chạy `make crash-test` (nếu làm bài B) $\rightarrow$ Báo ĐẠT.
- [ ] Điền đầy đủ thông tin vào [REPORT_TEMPLATE.md](file:///d:/AITHUCCHIEN/Labs/Day17-Track2-HaHoangTuanHung-2A202601629/REPORT_TEMPLATE.md) (bao gồm output `make verify`, số liệu P99, giải thích root cause).
- [ ] Kiểm tra tuyệt đối **KHÔNG** chỉnh sửa các file cấm: `expected/*`, `seed/generate.py`, `tools/verify.py`, `tools/explain.py`, `tools/common.py`.
- [ ] Chạy `make clean` để dọn dẹp các file cơ sở dữ liệu tạm trước khi nộp repo/nén file.
