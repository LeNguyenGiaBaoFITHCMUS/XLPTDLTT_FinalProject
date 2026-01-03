# HƯỚNG DẪN CHẠY PIPELINE XỬ LÝ GIAO DỊCH THẺ TÍN DỤNG

## Tổng quan Pipeline
```
Producer (Kafka) → Spark Streaming (HDFS) → Daily Aggregate (Warehouse) → Export (Power BI)
```

---

## Các chạy tự động hóa (Dùng khi đã hiểu rõ pipeline nha bà con)

### CÁC SCRIPTS TỰ ĐỘNG HÓA

Project có 3 scripts hỗ trợ chạy pipeline dễ dàng hơn:

#### 1. `setup_hdfs_permissions.sh` - Setup lần đầu
**Mục đích:** Tạo cấu trúc thư mục HDFS và cấp quyền

**Khi nào chạy:** 
- Lần đầu khởi động hệ thống
- Sau khi restart Docker containers
- Khi gặp lỗi "Permission denied" trên HDFS

**Cách chạy:**
```bash
chmod +x setup_hdfs_permissions.sh
./setup_hdfs_permissions.sh
```

**Chức năng:**
- Kiểm tra namenode container đã chạy chưa
- Tạo thư mục: `/datalake`, `/warehouse`, `/powerbi`, `/checkpoints`
- Cấp quyền 777 (rwxrwxrwx) cho tất cả thư mục
- Hiển thị kết quả kiểm tra quyền

#### 2. `run_pipeline.sh` - Chạy pipeline từng bước
**Mục đích:** Menu tương tác để chạy từng bước pipeline

**Khi nào chạy:**
- Khi muốn chạy thủ công từng bước
- Để debug hoặc test từng component
- Khi muốn kiểm tra dữ liệu giữa các bước

**Cách chạy:**
```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
```

**Menu options:**
```
Chọn bước muốn chạy:

  1. Chạy Producer (Kafka) - Gửi dữ liệu
  2. Chạy Spark Streaming - Xử lý real-time
  3. Kiểm tra dữ liệu trong HDFS
  4. Chạy Daily Aggregate - Phân tích
  5. Export cho Power BI
  6. Xem kết quả trong HDFS
  7. Chạy tất cả (tự động)
  0. Thoát
```

#### 3. `download_powerbi_data.sh` - Download dữ liệu về local
**Mục đích:** Download tất cả datasets từ HDFS về máy local để import vào Power BI

**Khi nào chạy:**
- Sau khi hoàn thành Export cho Power BI (Bước 5)
- Khi muốn phân tích dữ liệu trên Power BI Desktop

**Cách chạy:**
```bash
chmod +x download_powerbi_data.sh
./download_powerbi_data.sh
```

**Chức năng:**
- Tạo thư mục `./powerbi_data/` trên máy local
- Download 8 datasets từ `/powerbi/` trong HDFS
- Hiển thị kích thước file và cấu trúc
- Cleanup file tạm trong container
---

### THỨ TỰ CHẠY CHUẨN 

#### Lần đầu tiên (Fresh start):
```bash
# 1. Khởi động Docker
docker-compose up -d

# 2. Setup HDFS (chỉ chạy 1 lần)
./setup_hdfs_permissions.sh

# 3. Chạy pipeline (chọn option 7 - Chạy tất cả)
./run_pipeline.sh
# → Chọn: 7

# 4. Download dữ liệu cho Power BI
./download_powerbi_data.sh

# 5. Mở Power BI và import từ ./powerbi_data/
```

---

## CÁCH CHẠY THỦ CÔNG KHÔNG DÙNG FILE .sh (Mục đích để hiểu rõ quá trình đi của dữ liệu)

### BƯỚC 1: Khởi động hệ thống

```bash
# Khởi động tất cả containers
docker-compose up -d

# Kiểm tra trạng thái
docker ps

# Setup HDFS (sử dụng script)
./setup_hdfs_permissions.sh
```

### BƯỚC 2: Chạy Producer (Gửi dữ liệu vào Kafka)

Mở terminal mới và chạy:

```bash
docker exec -it spark-master python3 /opt/spark/apps/producer.py
```

**Chức năng:**
- Đọc file CSV giao dịch thẻ tín dụng
- Gửi từng dòng vào Kafka topic 'transactions'
- Random delay 1-5 giây giữa các dòng (theo yêu cầu)

**Output mẫu:**
```
[1] Đã gửi giao dịch: User 0 - Amount: $134.09
[2] Đã gửi giao dịch: User 0 - Amount: $89.23
```

### BƯỚC 3: Chạy Spark Streaming (Xử lý real-time)

Mở terminal mới và chạy:

```bash
docker exec -u root -it spark-master python3 /opt/spark/apps/spark_streaming.py
```

**Chức năng:**
- Consume dữ liệu từ Kafka
- Lọc giao dịch gian lận (Is Fraud? = No)
- Lấy tỷ giá USD-VND từ API/Web Scraping
- Chuyển đổi Amount từ USD sang VND
- Chuẩn hóa dữ liệu (ngày giờ, tên cột)
- **LƯU VÀO HDFS dạng Parquet** (partition theo ngày)

**Output mẫu:**
```
[INFO] Đã cập nhật tỷ giá USD-VND từ API: 25380
[INFO] Spark Streaming đang ghi dữ liệu vào HDFS: hdfs://namenode:8020/datalake/transactions_clean
[INFO] Dữ liệu được partition theo ngày (ds) để dễ phân tích
```

**Lưu ý:** Để Spark Streaming chạy liên tục (không tắt)

### BƯỚC 4: Kiểm tra dữ liệu trong HDFS

```bash
# Xem các partition (ngày)
docker exec namenode hdfs dfs -ls /datalake/transactions_clean

# Xem file parquet trong 1 ngày cụ thể
docker exec namenode hdfs dfs -ls /datalake/transactions_clean/ds=2026-01-03

# Đếm số dòng
docker exec spark-master spark-submit --master local[*] \
  --driver-memory 1g \
  --executor-memory 1g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3 \
  --py-files /dev/null <<EOF
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CountRecords").getOrCreate()
df = spark.read.parquet("hdfs://namenode:8020/datalake/transactions_clean")
print(f"Tổng số giao dịch: {df.count()}")
EOF
```

### BƯỚC 5: Chạy Daily Aggregate (Phân tích hàng ngày)

```bash
docker exec spark-master spark-submit /opt/spark/apps/daily_aggregate.py
```

### BƯỚC 6: Export cho Power BI

```bash
docker exec spark-master spark-submit /opt/spark/apps/export_powerbi.py
```

**Chức năng:**
- Export tất cả 8 loại phân tích vào thư mục `/powerbi/`
- Mỗi dataset được gộp thành 1 file CSV duy nhất (dễ import vào Power BI)

### BƯỚC 7: Download dữ liệu về để dùng Power BI

**Sử dụng script tự động (Khuyến nghị):**
```bash
./download_powerbi_data.sh
```

### BƯỚC 8: Airflow (Tự động hóa)

Airflow đã được cấu hình chạy tự động hàng ngày lúc 23:00.

**Truy cập Airflow UI:**
```
URL: http://localhost:8085
Username: admin
Password: vQNhZZK46Sk4FKq6
```

**DAG: creditcard_daily_pipeline_exec**
- Task 1: daily_aggregate (chạy spark-submit daily_aggregate.py)
- Task 2: export_powerbi (chạy spark-submit export_powerbi.py)
- Task 3: verify_hdfs_output (kiểm tra kết quả)

**Kích hoạt DAG:**
- Vào Airflow UI → DAGs → creditcard_daily_pipeline_exec → Toggle ON
- Hoặc chạy thủ công: Click "Trigger DAG"


## Cấu trúc dữ liệu trong HDFS

```
/
├── datalake/
│   └── transactions_clean/          # Dữ liệu từ Spark Streaming (Parquet)
│       ├── ds=2026-01-03/
│       ├── ds=2026-01-04/
│       └── ...
├── warehouse/                        # Kết quả phân tích (CSV)
│   ├── hourly_analysis/
│   ├── city_analysis/
│   ├── merchant_analysis/
│   ├── user_analysis/
│   ├── high_value_analysis/
│   ├── weekday_vs_weekend/
│   ├── error_analysis/
│   └── daily_summary/
├── powerbi/                          # Export cho Power BI (CSV)
│   ├── hourly_analysis/
│   ├── city_analysis/
│   └── ...
└── checkpoints/                      # Spark Streaming checkpoints
    └── transactions_clean/
```
---