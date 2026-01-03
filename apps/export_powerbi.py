from pyspark.sql import SparkSession

HDFS_WAREHOUSE = "hdfs://namenode:8020/warehouse"
HDFS_POWERBI = "hdfs://namenode:8020/powerbi"

def main():
    spark = SparkSession.builder.appName("ExportPowerBI").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("="*70)
    print("EXPORT DỮ LIỆU CHO POWER BI")
    print("="*70)
    
    # ============================================================
    # Export từng dataset riêng biệt
    # ============================================================
    datasets = [
        ("hourly_analysis", "Phân tích theo giờ"),
        ("city_analysis", "Phân tích theo thành phố"),
        ("merchant_analysis", "Phân tích theo merchant"),
        ("user_analysis", "Phân tích hành vi người dùng"),
        ("high_value_analysis", "Phân tích giao dịch giá trị cao"),
        ("weekday_vs_weekend", "So sánh ngày thường vs cuối tuần"),
        ("error_analysis", "Phân tích lỗi giao dịch"),
        ("daily_summary", "Tổng quan hàng ngày"),
        ("fraud_by_hour", "Phân tích fraud theo giờ"),
        ("fraud_by_city", "Phân tích fraud theo thành phố"),
        ("fraud_by_merchant", "Phân tích fraud theo merchant")
    ]
    
    for dataset_name, description in datasets:
        try:
            # Đọc từ warehouse
            df = spark.read.option("header", "true").csv(f"{HDFS_WAREHOUSE}/{dataset_name}")
            
            output_path = f"{HDFS_POWERBI}/{dataset_name}"
            
            (df.coalesce(1)  
               .write
               .mode("overwrite")
               .option("header", "true")
               .csv(output_path))
            
            print(f"[V] {description}: {output_path}")
            
        except Exception as e:
            print(f"[X] {description}: Lỗi - {str(e)}")
    
    print("\n" + "="*70)
    print("HOÀN THÀNH EXPORT")
    print("="*70)
    print("\nCác dataset đã được export vào thư mục:")
    print(f"  {HDFS_POWERBI}/")
    print("\nPower BI có thể kết nối trực tiếp vào HDFS hoặc download về local.")
    print("\nĐể download về local, chạy lệnh:")
    print(f"  docker exec namenode hdfs dfs -get {HDFS_POWERBI}/* ./powerbi_data/")

if __name__ == "__main__":
    main()
