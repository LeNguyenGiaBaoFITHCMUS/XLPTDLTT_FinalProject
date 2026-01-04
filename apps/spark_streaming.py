from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options 
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import time
import requests

# --- PHẦN CẤU HÌNH CHO DOCKER ---
chrome_options = Options()
chrome_options.add_argument("--headless")           # Chạy ẩn danh, không mở cửa sổ
chrome_options.add_argument("--no-sandbox")          # Vượt qua rào cản phân quyền của Linux
chrome_options.add_argument("--disable-dev-shm-usage") # Tránh crash do thiếu bộ nhớ đệm
chrome_options.add_argument("--disable-gpu")         # Tắt đồ họa (tối ưu cho server)
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")
# Khởi tạo driver với options đã cấu hình

def get_USD_price ():
    driver = webdriver.Chrome(
    service=ChromeService(ChromeDriverManager().install()),
    options=chrome_options # Gắn options vào đây
)
    url = "https://www.vietcombank.com.vn/vi-VN/KHCN/Cong-cu-Tien-ich/Ty-gia"
    driver.get(url)

    try:
        # Chờ trang web load lên bảng đầy đủ
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tbody tr"))
        )
        html = driver.page_source
    finally:
        driver.quit() # Luôn đóng driver để giải phóng RAM

    soup = BeautifulSoup(html, "lxml")
    rows = soup.find_all("tr")
    data = []

    row = rows[1]
    cols = row.find_all("td")
    # Kiểm tra xem dòng đó có đủ cột không (tránh lấy nhầm dòng tiêu đề hoặc dòng trống)
    if len(cols) >= 5:
        currency = cols[0].get_text(strip=True)
        buy_cash = cols[2].get_text(strip=True)
        buy_transfer = cols[3].get_text(strip=True)
        sell = cols[4].get_text(strip=True)
        data.append({
                "Mã ngoại tệ": currency,
                "Mua tiền mặt": buy_cash,
                "Mua chuyển khoản": buy_transfer,
                "Bán": sell
            })

    df = pd.DataFrame(data)
    return float(df.loc[0, 'Mua tiền mặt'].replace(',', ''))

def get_usd_rate_api():
    url = "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json"
    response = requests.get(url, timeout=10)
    data = response.json()
        
    return float(data['usd']['vnd'])


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import concat_ws, lpad, to_date, concat, lit, round, date_format, current_timestamp

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("KafkaHadoopStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3") \
    .getOrCreate()
# Giảm bớt log thừa để dễ quan sát dữ liệu
spark.sparkContext.setLogLevel("WARN")

# 2. Định nghĩa Schema khớp chính xác với dữ liệu bạn nhận được từ Kafka
schema = StructType([
    StructField("User", StringType(), True),
    StructField("Card", StringType(), True),
    StructField("Year", StringType(), True),
    StructField("Month", StringType(), True),
    StructField("Day", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("Amount", StringType(), True),
    StructField("Use Chip", StringType(), True),
    StructField("Merchant Name", StringType(), True),
    StructField("Merchant City", StringType(), True),
    StructField("Merchant State", StringType(), True),
    StructField("Zip", StringType(), True),
    StructField("MCC", StringType(), True),
    StructField("Errors?", StringType(), True),
    StructField("Is Fraud?", StringType(), True)
])

# 3. Đọc dữ liệu từ Kafka
# Lưu ý: dùng kafka:29092 vì Spark đang chạy trong mạng Docker
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Xử lý dữ liệu: Parse JSON và làm sạch cột Amount (xóa dấu $)
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Chuyển Amount từ "$134.09" thành số 134.09 để có thể tính toán
clean_df = parsed_df.withColumn("Amount", regexp_replace(col("Amount"), "\\$", "").cast("float"))
clean_df = clean_df.withColumn("User", col("User").cast("int")) \
                    .withColumn("Card", col("Card").cast("int")) \
                    .withColumn("Year", col("Year").cast("int")) \
                    .withColumn("Month", col("Month").cast("int")) \
                    .withColumn("Day", col("Day").cast("int")) \
                    .withColumn("Card", col("Card").cast("int")) \
                    .withColumn("Amount", col("Amount").cast("float")) \
                    .withColumn("MCC", col("MCC").cast("int"))

# GIỮ LẠI TẤT CẢ GIAO DỊCH (cả fraud và non-fraud) để phân tích
# Không filter bỏ fraud nữa vì cần phân tích xu hướng fraud (yêu cầu 4, 7)

# Xử lý cột Date và Time
processed_df = clean_df.withColumn("Date", concat_ws("/", lpad(col("Day"), 2, "0"), lpad(col("Month"), 2, "0"), col("Year"))).drop("Day", "Month", "Year")
processed_df = processed_df.withColumn("Date", to_date(col("Date"), "dd/MM/yyyy"))
processed_df = processed_df.withColumn("Time", concat(col("Time"), lit(":00")))

# Quản lý tỷ giá USD với cập nhật hàng ngày
class USDRateManager:
    def __init__(self):
        self.rate = None
        self.last_update = 0
        self.update_rate()
    
    def update_rate(self):
        try:
            self.rate = get_usd_rate_api()
        except:
            self.rate = get_USD_price()
        self.last_update = time.time()
        print(f"Đã cập nhật tỷ giá USD: {self.rate} VND")
    
    def get_rate(self):
        # Cập nhật nếu đã quá 24 giờ (86400 giây)
        # if time.time() - self.last_update > 86400:
        if time.time() - self.last_update > 20: # Demo thử nghiệm trong 20 giây
            self.update_rate()
        return self.rate

usd_manager = USDRateManager()

HDFS_OUTPUT_PATH = "hdfs://namenode:8020/datalake/transactions_clean"

# Hàm xử lý từng batch với tỷ giá cập nhật
def process_batch(batch_df, batch_id): # Thêm batch_id để khớp với yêu cầu Spark và tránh lỗi
    if batch_df.count() > 0:
        # Lấy tỷ giá
        current_rate = usd_manager.get_rate()
        
        # Chuyển đổi Amount từ USD sang VND
        result_df = batch_df.withColumn('amount_vnd', round(col('Amount') * current_rate).cast("int"))
        
        # Thêm cột partition theo ngày (ds) để dễ query và phân tích
        result_df = result_df.withColumn("ds", date_format(col("Date"), "yyyy-MM-dd"))
        
        # Đổi tên cột để chuẩn hóa (lowercase, snake_case)
        result_df = result_df.withColumnRenamed("User", "user_id") \
                           .withColumnRenamed("Card", "card_id") \
                           .withColumnRenamed("Time", "time") \
                           .withColumnRenamed("Amount", "amount_usd") \
                           .withColumnRenamed("Use Chip", "use_chip") \
                           .withColumnRenamed("Merchant Name", "merchant_name") \
                           .withColumnRenamed("Merchant City", "merchant_city") \
                           .withColumnRenamed("Merchant State", "merchant_state") \
                           .withColumnRenamed("Zip", "zip") \
                           .withColumnRenamed("MCC", "mcc") \
                           .withColumnRenamed("Errors?", "has_errors") \
                           .withColumnRenamed("Is Fraud?", "is_fraud") \
                           .withColumnRenamed("Date", "transaction_date")
        
        # Thêm timestamp xử lý
        result_df = result_df.withColumn("processed_at", current_timestamp())
        
        # Ghi vào HDFS dạng Parquet, partition theo ngày
        result_df.write \
            .mode("append") \
            .format("parquet") \
            .partitionBy("ds") \
            .save(HDFS_OUTPUT_PATH)

# Ghi vào HDFS với foreachBatch
query = processed_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "hdfs://namenode:8020/datalake/checkpoints/transactions_clean") \
    .start()

print(f"[INFO] Spark Streaming đang ghi dữ liệu vào HDFS: {HDFS_OUTPUT_PATH}")
print("[INFO] Dữ liệu được partition theo ngày (ds) để dễ phân tích")
print("[INFO] Nhấn Ctrl+C để dừng...")

# LỆNH QUAN TRỌNG: Giữ chương trình chạy liên tục
query.awaitTermination()