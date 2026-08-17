# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Hà Hoàng Tuấn Hùng  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details open>
<summary>Output ba lần chạy verify</summary>

```text
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 43.4s
  run 2/3 … 47.9s
  run 3/3 … 46.4s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt** (100/100 điểm chính + 10/10 điểm thưởng bài mở rộng)

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau khi Airflow task bị lỗi mạng và người trực bấm Clear Task, hoặc khi chạy lại pipeline, số hàng trong `gold_training_set` tăng gấp bội (từ 12.480 lên 38.750+ hàng), 12.480 ticket bị nhân bản nhiều lần dù không có lỗi phát sinh. |
| **Nguyên nhân** | Model `gold_training_set.sql` được cấu hình `materialized='incremental'` nhưng **thiếu `unique_key` và `incremental_strategy`**, khiến dbt mặc định dùng câu lệnh `INSERT INTO` (append). Bảng nguồn CDC có các bản ghi cập nhật (`op='u'`) ở các ngày khác nhau, khiến một ticket đi qua điều kiện lọc theo partition ngày nhiều lần trong cùng một lượt chạy và bị chèn thêm các hàng mới thay vì hợp nhất/ghi đè (merge). Đồng thời trong DAG Airflow, `catchup=True` và `max_active_runs` không giới hạn khiến thao tác Clear Task kích hoạt nhiều run chạy bù đồng thời, làm nhân bản dữ liệu hàng loạt. |
| **Cách khắc phục** | - File [dbt/models/gold/gold_training_set.sql](dbt/models/gold/gold_training_set.sql): Thêm `unique_key = 'ticket_id'`, `incremental_strategy = 'merge'`.<br>- File [dags/ai_training_pipeline.py](dags/ai_training_pipeline.py): Cấu hình `catchup = False`, `max_active_runs = 1`. |
| **Bằng chứng** | trước: 38.750 hàng (12.480 ticket bị lặp, checksum lệch qua từng lượt) · sau: **12.480 hàng** (đúng 1 hàng / 1 ticket không lặp) · checksum 3 lượt đồng nhất: `8dd7c98653` |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` bị thiếu ~5% số hàng so với đối chiếu thủ công (8.645 / 9.100 hàng). Số hàng thiếu tập trung ở các ngày trong quá khứ đã hoàn thành từ lâu, trong khi các ngày gần nhất lại đủ. |
| **P99 độ trễ đo được** | **3.0 ngày** *(Đo qua phân bố `date_diff('second', event_time, _ingested_at)/86400.0` trên bảng `bronze_events`)* |
| **Lookback đã chọn** | **3 ngày** — vì độ trễ P99 đo được là 3.0 ngày, bao phủ 99% các bản ghi về muộn mà không tốn chi phí quét lại toàn bộ lịch sử. |
| **Nguyên nhân** | Điều kiện lọc incremental hiện tại là `where event_date > (select max(event_date) from {{ this }})`. Cơ chế này chỉ chấp nhận các sự kiện có ngày phát sinh mới hơn ngày lớn nhất đã có trong bảng đích. Khi một sự kiện phát sinh ngày 08-12 nhưng về kho muộn vào ngày 08-15 (trễ 3 ngày), tại thời điểm chạy ngày 08-15 thì `max(event_date)` trong đích đã là 08-14/08-15, khiến điều kiện `>` lọc bỏ vĩnh viễn sự kiện này. |
| **Cách khắc phục** | - File [dbt/models/gold/gold_feature_daily.sql](dbt/models/gold/gold_feature_daily.sql): Khai báo composite key `unique_key = ['event_date', 'customer_id']`, `incremental_strategy = 'merge'`.<br>- Mở rộng lookback window: `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`. |
| **Bằng chứng** | trước: 8.645 hàng (thiếu 455 hàng) · sau: **9.100 hàng** (khớp đúng 14 ngày × 650 customer) · checksum 3 lượt đồng nhất: `3db448685c` |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> - **Nếu chọn `max`:** Giá trị `max` thường bị kéo dài bất thường bởi các ngoại lai (outliers), lỗi đồng hồ thiết bị hoặc dữ liệu rác (ví dụ một sự kiện bị lệch hàng tháng/hàng năm). Hậu quả là tại **mọi** lượt chạy định kỳ, pipeline đều bị ép phải quét lại toàn bộ lịch sử (Full scan & Heavy Merge), gây lãng phí nghiêm trọng tài nguyên I/O, CPU và kéo dài thời gian xử lý.
> - **Nếu chọn `P99`:** Cân bằng tối ưu giữa độ chính xác dữ liệu và chi phí tính toán. Cửa sổ 3 ngày bao phủ trọn vẹn 99% dữ liệu về muộn với chi phí tính toán cố định và rất nhỏ trong mỗi batch. 1% cực đoan còn lại sẽ được xử lý qua các job đối soát/backfill định kỳ riêng (Reconciliation) mà không làm chậm pipeline vận hành hàng ngày.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Backend đổi kiểu `priority` từ số sang chuỗi (`urgent`, `high`, `medium`, `low`) từ ngày 08-10. Pipeline không dừng nhưng model phân loại AI dự đoán kém đi rõ rệt; `silver_tickets.priority` chứa 6.606 hàng sai (chứa NULL và các giá trị ngoài 1..4 như 0, 5, -1); `quarantine_tickets` có 0 hàng. |
| **Nguyên nhân** | - Backend thực hiện Schema Evolution đổi định dạng từ số sang chuỗi.<br>- Hàm `try_cast(priority_raw as integer)` cũ gây lỗi hai chiều: vừa biến các nhãn chuỗi hợp lệ thành `NULL` (vứt bỏ dữ liệu tốt), vừa chấp nhận các số ngoài hợp đồng như `0`, `5`, `-1` (vì chúng là integer hợp lệ).<br>- Chưa kích hoạt Data Contract (`enforced: false`) và thiếu test miền giá trị `accepted_values: [1, 2, 3, 4]`.<br>- Trong `silver_tickets.sql`, nếu xếp hạng `row_number()` trước khi lọc lỗi thì một ticket có bản ghi mới nhất bị lỗi sẽ làm mất luôn cả ticket đó khỏi tầng Silver (số ticket tụt xuống 12.168). |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | 1. **Nhóm 1 (Số hợp lệ 1..4):** `'1'`, `'2'`, `'3'`, `'4'` $\rightarrow$ Ép kiểu sang số nguyên và giữ nguyên trong Silver.<br>2. **Nhóm 2 (Nhãn chuỗi hợp lệ):** `'urgent'`, `'high'`, `'medium'`, `'low'` $\rightarrow$ Chuẩn hoá ánh xạ về số: `urgent=1`, `high=2`, `medium=3`, `low=4`.<br>3. **Nhóm 3 (Dữ liệu lỗi thật sự):** `'P1'`, `'unknown'`, `'0'`, `'5'`, `'-1'`, `''`, `NULL` $\rightarrow$ Trả về `NULL` trong macro để định tuyến vào `quarantine_tickets`. |
| **Cách khắc phục** | - [dbt/macros/normalize_priority.sql](dbt/macros/normalize_priority.sql): Dùng `CASE WHEN` chuẩn hoá 3 nhóm giá trị, trả về `NULL` cho nhóm 3; bổ sung `priority_reject_reason`.<br>- [dbt/models/silver/silver_tickets.sql](dbt/models/silver/silver_tickets.sql): Lọc bỏ bản ghi lỗi (`where priority_clean is not null`) **trước** khi đánh số thứ tự `row_number()`.<br>- [dbt/models/silver/quarantine_tickets.sql](dbt/models/silver/quarantine_tickets.sql): Đổi điều kiện thành `where normalize_priority(priority_raw) is null`.<br>- [dbt/models/silver/schema.yml](dbt/models/silver/schema.yml): Đổi `enforced: true` và bật test `not_null`, `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = **312 hàng** (đúng 312/312) · `silver_tickets` đủ **12.480 ticket** · `silver_tickets.priority` sạch 100% không NULL · `dbt test` **11/11 pass**. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> - **Nên chặn ở tầng Silver thay vì Bronze:** Tầng Bronze đóng vai trò là Data Lake / Raw Ingestion bất biến (Immutable Log) lưu trữ trung thực 100% dữ liệu gốc nhận từ nguồn. Nếu từ chối bản ghi ngay tại Bronze, dữ liệu bị mất vĩnh viễn, không thể phục hồi, điều tra nguyên nhân gốc hay tái xử lý khi sau này đội backend cung cấp quy tắc ánh xạ mới.
> - **Không để pipeline dừng (fail-fast) khi gặp bản ghi lỗi:** Trong 14.300 bản ghi CDC và hơn 130.000 sự kiện, chỉ có 312 bản ghi bị lỗi priority (~2%). Nếu dừng cả DAG, 98% dữ liệu hợp lệ còn lại của hàng nghìn khách hàng sẽ bị tắc nghẽn, làm tê liệt toàn bộ hệ thống phục vụ người dùng thời gian thực. Việc áp dụng **Quarantine Pattern** (cách ly bản ghi lỗi vào bảng riêng) giúp luồng chính tiếp tục chạy thông suốt, đồng thời tạo hàng đợi cho đội vận hành kiểm tra và xử lý sau.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | **Cả 2 bài: Bài A (Tối ưu Dashboard) và Bài B (Consumer Crash Recovery)** |
| **Nguyên nhân** | - **Bài A:** Small-file problem (5.000 file Parquet nhỏ, mỗi file vài chục KB) gây chi phí mở file và đọc metadata khổng lồ, kết hợp điều kiện non-sargable `strftime(event_time, '%Y-%m-%d') = '2026-08-09'` khiến engine không dùng được min/max statistics để prune file/row group.<br>- **Bài B:** Ngữ nghĩa At-most-once (commit offset trước khi ghi dữ liệu) khiến mất mát dữ liệu khi tiến trình bị kill giữa chừng. Nếu chuyển sang At-least-once (ghi trước, commit sau) mà dùng `INSERT` thuần thì khi restart replay batch sẽ gây trùng lặp dữ liệu. |
| **Cách khắc phục** | - **Bài A:** Dùng [tools/compact.py](tools/compact.py) gom nhóm và ghi ra `data/gold_events_v2` với `PARTITION_BY (event_date)`, sắp xếp `ORDER BY customer_name, event_time`, `ROW_GROUP_SIZE 10000`. Cập nhật [queries/dashboard.sql](queries/dashboard.sql) trỏ vào `data/gold_events_v2/*/*.parquet` (`hive_partitioning = 1`) và đổi điều kiện lọc sargable `event_date = '2026-08-09'`.<br>- **Bài B:** Thêm `PRIMARY KEY (event_id)` vào DDL `bronze_events_stream`, chuyển lệnh ghi thành Idempotent Upsert (`INSERT ... ON CONFLICT (event_id) DO UPDATE SET ...`), và sắp xếp lại thứ tự trong `consume()`: `write_batch` $\rightarrow$ `maybe_crash` $\rightarrow$ `consumer.commit()`. |
| **Bằng chứng** | - **Bài A:** `rows scanned` giảm từ 5.000.000 xuống **9.324** (giảm **536.3×**, vượt xa yêu cầu $\ge 10\times$); số file giảm từ 5.000 xuống **14 file**; `result hash` giữ nguyên `4379e4c5d9f3`.<br>- **Bài B:** `make crash-test` báo `BÀI MỞ RỘNG B: ĐẠT ✓` (0 mất hàng, 0 trùng hàng, $C == A = 20.000$ hàng). |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra các model `materialized='incremental'` có khai báo đầy đủ `unique_key` và chiến lược `merge` phù hợp với grain của bảng hay không; kiểm tra cấu hình scheduler DAG (`catchup=False`, `max_active_runs=1`) để đảm bảo pipeline có tính **Idempotent** (chạy lại N lần vẫn cho một kết quả duy nhất). |
| 2 | Phân tích phân bố độ trễ truyền nhận của dữ liệu nguồn ($P50, P95, P99$) và kiểm tra điều kiện lọc `is_incremental()` để thiết lập **Lookback Window** hợp lý, tránh tình trạng bỏ rơi dữ liệu về muộn (Late-arriving data). |
| 3 | Kiểm tra việc áp dụng **Data Contract** (`enforced: true`), các bộ test ràng buộc miền giá trị (accepted values), và thiết kế kiến trúc **Quarantine Pattern** để phân loại và định tuyến bản ghi lỗi sang bảng cách ly thay vì để pipeline dừng đột ngột. |
