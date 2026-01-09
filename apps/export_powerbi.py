from pyspark.sql import SparkSession

HDFS_WAREHOUSE = "hdfs://namenode:8020/warehouse"
HDFS_POWERBI = "hdfs://namenode:8020/powerbi"

def rename_part_file(spark, temp_path, final_path):
    # Hàm hỗ trợ đổi tên file part-00000... thành tên mong muốn bằng Hadoop FS API
    # Lấy đối tượng Hadoop FileSystem từ JVM
    sc = spark.sparkContext
    # Tạo URI cho HDFS
    java_uri = sc._jvm.java.net.URI("hdfs://namenode:8020")
    # Lấy FileSystem từ URI
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(java_uri, sc._jsc.hadoopConfiguration())
    Path = sc._jvm.org.apache.hadoop.fs.Path

    # KIỂM TRA VÀ XÓA FILE ĐÍCH CŨ (QUAN TRỌNG)
    dest_path_obj = Path(final_path)
    if fs.exists(dest_path_obj):
        fs.delete(dest_path_obj, False) # False: không cần xóa đệ quy vì là file đơn
        print(f"[Info] Đã xóa file cũ: {final_path}")

    # Tìm file part- trong thư mục tạm để di chuyển
    src_path_obj = Path(temp_path)
    if fs.exists(src_path_obj):
        files = fs.listStatus(src_path_obj)
        found = False
        for f in files:
            file_name = f.getPath().getName()
            if file_name.startswith("part-") and file_name.endswith(".csv"):
                # Di chuyển và đổi tên file
                success = fs.rename(f.getPath(), dest_path_obj)
                if success:
                    print(f"-> Đã đổi tên thành công")
                else:
                    print(f"[ERROR] Không thể đổi tên file sang {final_path}")
                found = True
                break
        
        if not found:
            print(f"[WARN] Không tìm thấy file part- csv nào trong {temp_path}")
        
        # Xóa thư mục tạm sau khi đã lấy file
        fs.delete(src_path_obj, True)
    else:
        print(f"[WARN] Không tìm thấy thư mục tạm: {temp_path}")

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
        ("hourly_analysis/day", "day_hourly_analysis"),
        ("hourly_analysis/month", "month_hourly_analysis"),
        ("hourly_analysis/year", "year_hourly_analysis"),
        ("hourly_unusual_analysis/day", "day_hourly_unusual_analysis"),
        ("hourly_unusual_analysis/month", "month_hourly_unusual_analysis"),
        ("hourly_unusual_analysis/year", "year_hourly_unusual_analysis"),
        ("city_analysis/day", "day_city_analysis"),
        ("city_analysis/month", "month_city_analysis"),
        ("city_analysis/year", "year_city_analysis"),
        ("merchant_analysis/day", "day_merchant_analysis"),
        ("merchant_analysis/month", "month_merchant_analysis"),
        ("merchant_analysis/year", "year_merchant_analysis"),
        ("user_analysis", "user_analysis"),
        ("high_value_analysis/day", "day_high_value_analysis"),
        ("high_value_analysis/month", "month_high_value_analysis"),
        ("high_value_analysis/year", "year_high_value_analysis"),
        ("weekday_vs_weekend", "weekday_vs_weekend"),
        ("user_error_analysis/day", "day_user_error_analysis"),
        ("user_error_analysis/month", "month_user_error_analysis"),
        ("user_error_analysis/year", "year_user_error_analysis"),
        ("user_fraud_analysis/day", "day_user_fraud_analysis"),
        ("user_fraud_analysis/month", "month_user_fraud_analysis"),
        ("user_fraud_analysis/year", "year_user_fraud_analysis"),
        ("fraud_by_hour/day", "day_fraud_by_hour"),
        ("fraud_by_hour/month", "month_fraud_by_hour"),
        ("fraud_by_hour/year", "year_fraud_by_hour"),
        ("fraud_by_merchant/day", "day_fraud_by_merchant"),
        ("fraud_by_merchant/month", "month_fraud_by_merchant"),
        ("fraud_by_merchant/year", "year_fraud_by_merchant"),
        ("fraud_by_city/day", "day_fraud_by_city"),
        ("fraud_by_city/month", "month_fraud_by_city"),
        ("fraud_by_city/year", "year_fraud_by_city"),
        ("daily_summary", "daily_summary")
    ]
    
    for dataset_path, filename_base in datasets:
        try:
            # Đọc từ warehouse
            df = spark.read.option("header", "true").csv(f"{HDFS_WAREHOUSE}/{dataset_path}")
            
            # Nơi Spark ghi tạm (dạng folder)
            temp_path = f"{HDFS_POWERBI}/temp_{filename_base}"
            # Tên file CSV cuối cùng
            final_path = f"{HDFS_POWERBI}/{filename_base}.csv"
            
            (df.coalesce(1)  
               .write
               .mode("overwrite")
               .option("header", "true")
               .csv(temp_path))
            
            # Đổi tên file part-... trong temp thành file đích
            rename_part_file(spark, temp_path, final_path)

            print(f"[V] {filename_base}: {final_path}")
            
        except Exception as e:
            print(f"[X] {filename_base}: Lỗi - {str(e)}")
    
    print("\n" + "="*70)
    print("HOÀN THÀNH EXPORT")
    print("="*70)
    print(f"\nCác dataset đã được export vào thư mục: {HDFS_POWERBI}/")

if __name__ == "__main__":
    main()