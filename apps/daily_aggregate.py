from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as Fsum, count as Fcount, avg as Favg, max as Fmax, min as Fmin,
    hour, dayofweek, when, countDistinct, desc, round as Fround
)

HDFS_CLEAN = "hdfs://namenode:8020/datalake/transactions_clean"
HDFS_WAREHOUSE = "hdfs://namenode:8020/warehouse"

def main():
    spark = SparkSession.builder.appName("DailyAggregate").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(HDFS_CLEAN)
    
    print(f"[INFO] Đọc được {df.count()} giao dịch từ HDFS")
    
    # ============================================================
    # YÊU CẦU 1: Phân tích theo GIỜ - Tìm khung giờ có nhiều giao dịch nhất
    # ============================================================
    df_with_hour = df.withColumn("hour", hour(col("time")))
    
    # === Theo ngày ===
    # Tìm ngày mới nhất
    latest_ds = df.select(Fmax("ds")).collect()[0][0]
    
    # Lọc chỉ lấy dữ liệu của ngày mới nhất
    df_day_latest = df_with_hour.filter(col("ds") == latest_ds)
    
    # Thống kê số lượng giao dịch theo giờ
    day_hourly_stats = (df_day_latest.groupBy("hour")
                    .agg(Fcount("*").alias("transaction_count"))
                    .orderBy(desc("transaction_count")))
    
    (day_hourly_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/hourly_analysis/day"))
    
    # Thống kê số lượng giao dịch bất thường (has_errors != "") theo giờ
    df_errors_day_latest = df_day_latest.filter(col("has_errors") != "")
    day_hourly_error_stats = (df_errors_day_latest.groupBy("hour")
                          .agg(Fcount("*").alias("error_transaction_count"))
                          .orderBy(desc("error_transaction_count")))
    
    (day_hourly_error_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/hourly_unusual_analysis/day"))
    
    # === Theo tháng ===
    # Tìm tháng-năm mới nhất
    latest_month_year = df_with_hour.select(Fmax(col("ds").substr(1, 7))).collect()[0][0]

    # Lọc chỉ lấy dữ liệu của tháng-năm mới nhất
    df_month_latest = df_with_hour.filter(col("ds").substr(1, 7) == latest_month_year)

    # Thống kê số lượng giao dịch theo giờ
    month_hourly_stats = (df_month_latest.groupBy("hour")
                      .agg(Fcount("*").alias("transaction_count"))
                      .orderBy(desc("transaction_count")))
    
    (month_hourly_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/hourly_analysis/month"))
    
    # Thống kê số lượng giao dịch bất thường (has_errors != "") theo giờ
    df_errors_month_latest = df_month_latest.filter(col("has_errors") != "")
    month_hourly_error_stats = (df_errors_month_latest.groupBy("hour")
                            .agg(Fcount("*").alias("error_transaction_count"))
                            .orderBy(desc("error_transaction_count")))
    
    (month_hourly_error_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/hourly_unusual_analysis/month"))
    
     # === Theo năm ===
    # Tìm năm mới nhất
    latest_year = df_with_hour.select(Fmax(col("ds").substr(1, 4))).collect()[0][0]

    # Lọc chỉ lấy dữ liệu của năm mới nhất
    df_year_latest = df_with_hour.filter(col("ds").substr(1, 4) == latest_year)

    # Thống kê số lượng giao dịch theo giờ
    year_hourly_stats = (df_year_latest.groupBy("hour")
                     .agg(Fcount("*").alias("transaction_count"))
                     .orderBy(desc("transaction_count")))
    
    (year_hourly_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/hourly_analysis/year"))
    
    # Thống kê số lượng giao dịch bất thường (has_errors != "") theo giờ
    df_errors_year_latest = df_year_latest.filter(col("has_errors") != "")
    year_hourly_error_stats = (df_errors_year_latest.groupBy("hour")
                            .agg(Fcount("*").alias("error_transaction_count"))
                            .orderBy(desc("error_transaction_count")))
    
    (year_hourly_error_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/hourly_unusual_analysis/year"))
    
    print("[V] Phân tích theo giờ - Hoàn thành")
    
    # ============================================================
    # YÊU CẦU 2: Phân tích theo THÀNH PHỐ - Tìm thành phố có giá trị cao nhất
    # ============================================================
    # === Theo ngày ===
    day_city_stats = (df_day_latest.groupBy("merchant_city", "merchant_state")
                    .agg(
                        Fcount("*").alias("transaction_count"),
                        Fsum("amount_vnd").alias("total_amount_vnd"),
                        Favg("amount_vnd").alias("avg_amount_vnd"))
                    .orderBy(desc("total_amount_vnd")))
    
    (day_city_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/city_analysis/day"))
    
    # === Theo tháng ===
    month_city_stats = (df_month_latest.groupBy("merchant_city", "merchant_state")
                      .agg(
                          Fcount("*").alias("transaction_count"),
                          Fsum("amount_vnd").alias("total_amount_vnd"),
                          Favg("amount_vnd").alias("avg_amount_vnd"))
                      .orderBy(desc("total_amount_vnd")))
    
    (month_city_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/city_analysis/month"))
    
    # === Theo năm ===
    year_city_stats = (df_year_latest.groupBy("merchant_city", "merchant_state")
                     .agg(
                         Fcount("*").alias("transaction_count"),
                         Fsum("amount_vnd").alias("total_amount_vnd"),
                         Favg("amount_vnd").alias("avg_amount_vnd"))
                     .orderBy(desc("total_amount_vnd")))
    
    (year_city_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/city_analysis/year"))
    
    print("[V] Phân tích theo thành phố - Hoàn thành")
    
    # ============================================================
    # YÊU CẦU 3: Phân tích theo MERCHANT - Số lượng và giá trị giao dịch
    # ============================================================
    # === Theo ngày ===
    day_merchant_stats = (df_day_latest.groupBy("merchant_name", "merchant_city", "merchant_state")
                        .agg(
                            Fcount("*").alias("transaction_count"),
                            Fsum("amount_vnd").alias("total_amount_vnd"),
                            Favg("amount_vnd").alias("avg_amount_vnd"),
                            Fmax("amount_vnd").alias("max_amount_vnd"),
                            countDistinct("user_id").alias("unique_customers"))
                        .orderBy(desc("total_amount_vnd")))
    
    (day_merchant_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/merchant_analysis/day"))
    
    # === Theo tháng ===
    month_merchant_stats = (df_month_latest.groupBy("merchant_name", "merchant_city", "merchant_state")
                            .agg(
                                Fcount("*").alias("transaction_count"),
                                Fsum("amount_vnd").alias("total_amount_vnd"),
                                Favg("amount_vnd").alias("avg_amount_vnd"),
                                Fmax("amount_vnd").alias("max_amount_vnd"),
                                countDistinct("user_id").alias("unique_customers"))
                            .orderBy(desc("total_amount_vnd")))
    
    (month_merchant_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/merchant_analysis/month"))
    
    # === Theo năm ===
    year_merchant_stats = (df_year_latest.groupBy("merchant_name", "merchant_city", "merchant_state")
                           .agg(
                               Fcount("*").alias("transaction_count"),
                               Fsum("amount_vnd").alias("total_amount_vnd"),
                               Favg("amount_vnd").alias("avg_amount_vnd"),
                               Fmax("amount_vnd").alias("max_amount_vnd"),
                               countDistinct("user_id").alias("unique_customers"))
                           .orderBy(desc("total_amount_vnd")))
    
    (year_merchant_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/merchant_analysis/year"))
    
    print("[V] Phân tích theo merchant - Hoàn thành")
    
    # ============================================================
    # YÊU CẦU 4: Phân tích USER BEHAVIOR - Người dùng có nhiều giao dịch
    # ============================================================
    user_stats = (df.groupBy("ds", "user_id")
                  .agg(
                      Fcount("*").alias("transaction_count"),
                      Fsum("amount_vnd").alias("total_spent_vnd"),
                      Favg("amount_vnd").alias("avg_transaction_vnd"),
                      countDistinct("merchant_name").alias("unique_merchants"),
                      countDistinct("merchant_city").alias("unique_cities"),
                      Fsum(when(col("has_errors") != "", 1).otherwise(0)).alias("error_count")
                  )
                  .orderBy(desc("transaction_count")))
    
    (user_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/user_analysis"))
    
    print("[V] Phân tích user behavior - Hoàn thành")

    # Phân tích người dùng có nhiều giao dịch liên tiếp (cùng hour + ds)
    df_with_hour = df.withColumn("hour", hour(col("time")))
    user_frequent_stats = (df_with_hour.groupBy("ds", "hour", "user_id")
                          .agg(Fcount("*").alias("transaction_count"))
                          .filter(col("transaction_count") > 1)  # Lọc >1 giao dịch
                          .groupBy("user_id")
                          .agg(
                              Fcount("*").alias("frequent_hour_count"),
                              Fsum("transaction_count").alias("total_rapid_transactions"),
                              Favg("transaction_count").alias("avg_txn_per_hour")
                          )
                          .orderBy(desc("frequent_hour_count")))
    
    (user_frequent_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/user_frequent"))
    
    print("[V] Phân tích người dùng giao dịch liên tiếp - Hoàn thành")
    
    
    # ============================================================
    # YÊU CẦU 5: Giao dịch GIÁ TRỊ LỚN - Tìm pattern của giao dịch lớn
    # ============================================================
    quantiles = df.stat.approxQuantile("amount_vnd", [0.95], 0.01)
    high_value_threshold = quantiles[0] if quantiles else 0

    # === Theo ngày ===
    day_high_val_trans_stats = (df_day_latest.filter(col("amount_vnd") >= high_value_threshold)
                                .groupBy("hour", "merchant_city", "merchant_name")
                                .agg(
                                    Fcount("*").alias("high_value_count"),
                                    Fmax("amount_vnd").alias("max_amount_vnd"))
                                .orderBy(desc("high_value_count")))
    
    (day_high_val_trans_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/high_value_analysis/day"))
    
    # === Theo tháng ===
    month_high_val_trans_stats = (df_month_latest.filter(col("amount_vnd") >= high_value_threshold)
                                  .groupBy("hour", "merchant_city", "merchant_name")
                                  .agg(
                                      Fcount("*").alias("high_value_count"),
                                      Fmax("amount_vnd").alias("max_amount_vnd"))
                                  .orderBy(desc("high_value_count")))
    
    (month_high_val_trans_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/high_value_analysis/month"))
    
    # === Theo năm ===
    year_high_val_trans_stats = (df_year_latest.filter(col("amount_vnd") >= high_value_threshold)
                                 .groupBy("hour", "merchant_city", "merchant_name")
                                 .agg(
                                     Fcount("*").alias("high_value_count"),
                                     Fmax("amount_vnd").alias("max_amount_vnd"))
                                 .orderBy(desc("high_value_count")))
    
    (year_high_val_trans_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/high_value_analysis/year"))
    
    print(f"[V] Phân tích giao dịch giá trị lớn (>= {high_value_threshold:,.0f} VND) - Hoàn thành")
    
    # ============================================================
    # YÊU CẦU 6: So sánh NGÀY THƯỜNG vs CUỐI TUẦN
    # ============================================================
    df_with_dayofweek = df_with_hour.withColumn("day_of_week", dayofweek(col("transaction_date")))
    df_with_daytype_hour = df_with_dayofweek.withColumn(
        "day_type",
        when((col("day_of_week") == 1) | (col("day_of_week") == 7), "Weekend")
        .otherwise("Weekday")
    )
    
    # Tính số giao dịch theo từng ngày và day_type
    daily_transactions_by_type = (df_with_daytype_hour.groupBy("ds", "day_type")
                                  .agg(Fcount("*").alias("daily_transaction_count")))
    
    # Tính số giao dịch trung bình mỗi ngày cho mỗi day_type
    avg_transactions_per_day = (daily_transactions_by_type.groupBy("day_type")
                                .agg(Favg("daily_transaction_count").alias("avg_transactions_per_day")))
    
    weekday_comparison = (df_with_daytype_hour.groupBy("day_type")
                          .agg(
                              Fcount("*").alias("transaction_count"),
                              Fsum("amount_vnd").alias("total_amount_vnd"),
                              Favg("amount_vnd").alias("avg_amount_vnd"),
                              countDistinct("ds").alias("unique_days")
                          ))
    
    # Join với avg_transactions_per_day
    weekday_comparison = weekday_comparison.join(avg_transactions_per_day, "day_type", "left")
    
    (weekday_comparison.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/weekday_vs_weekend"))
    
    print("[V] So sánh ngày thường vs cuối tuần - Hoàn thành")
    
    # ============================================================
    # YÊU CẦU 7: Phân tích LỖI và GIAN LẬN - User có nhiều tình trạng bất thường
    # ============================================================
    # === Theo ngày ===
    # Phân tích lỗi giao dịch theo user
    day_user_error_stats = (df_errors_day_latest.groupBy("user_id")
                            .agg(
                                Fcount("*").alias("error_count"),
                                Fsum("amount_vnd").alias("total_error_amount_vnd"))
                            .orderBy(desc("error_count")))
    
    (day_user_error_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/user_error_analysis/day"))
    
    # Phân tích gian lận theo user
    df_fraud_day_latest = df_day_latest.filter(col("is_fraud") == "Yes")
    day_user_fraud_stats = (df_fraud_day_latest.groupBy("user_id")
                            .agg(Fcount("*").alias("fraud_count"))
                            .orderBy(desc("fraud_count")))
    
    (day_user_fraud_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/user_fraud_analysis/day"))
    
     # === Theo tháng ===
    # Phân tích lỗi giao dịch theo user
    month_user_error_stats = (df_errors_month_latest.groupBy("user_id")
                            .agg(
                                Fcount("*").alias("error_count"),
                                Fsum("amount_vnd").alias("total_error_amount_vnd"))
                            .orderBy(desc("error_count")))
    
    (month_user_error_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/user_error_analysis/month"))
    
    # Phân tích gian lận theo user
    df_fraud_month_latest = df_month_latest.filter(col("is_fraud") == "Yes")
    month_user_fraud_stats = (df_fraud_month_latest.groupBy("user_id")
                            .agg(Fcount("*").alias("fraud_count"))
                            .orderBy(desc("fraud_count")))
    
    (month_user_fraud_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/user_fraud_analysis/month"))
    
     # === Theo năm ===
    # Phân tích lỗi giao dịch theo user
    year_user_error_stats = (df_errors_year_latest.groupBy("user_id")
                            .agg(
                                Fcount("*").alias("error_count"),
                                Fsum("amount_vnd").alias("total_error_amount_vnd"))
                            .orderBy(desc("error_count")))
    
    (year_user_error_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/user_error_analysis/year"))
    
    # Phân tích gian lận theo user
    df_fraud_year_latest = df_year_latest.filter(col("is_fraud") == "Yes")
    year_user_fraud_stats = (df_fraud_year_latest.groupBy("user_id")
                            .agg(Fcount("*").alias("fraud_count"))
                            .orderBy(desc("fraud_count")))
    
    (year_user_fraud_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/user_fraud_analysis/year"))
    
    print("[V] Phân tích người dùng theo lỗi và gian lận - Hoàn thành")
    
    # ============================================================
    # YÊU CẦU 4 & 7: Phân tích FRAUD - Xu hướng fraud theo giờ, city, merchant
    # ============================================================
    # === Theo ngày ===
    # Phân tích fraud theo giờ
    day_fraud_by_hour_stats = (df_fraud_day_latest.groupBy("hour")
                                .agg(Fcount("*").alias("fraud_count"))
                                .orderBy(desc("fraud_count")))

    (day_fraud_by_hour_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/fraud_by_hour/day"))
    
    # Phân tích fraud theo merchant
    day_fraud_by_merchant_stats = (df_fraud_day_latest.groupBy("merchant_name", "merchant_city")
                                    .agg(Fcount("*").alias("fraud_count"))
                                    .orderBy(desc("fraud_count")))
    
    (day_fraud_by_merchant_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/fraud_by_merchant/day"))
    
    # Phân tích fraud theo city
    day_fraud_by_city_stats = (df_fraud_day_latest.groupBy("merchant_city", "merchant_state")
                                .agg(Fcount("*").alias("fraud_count"))
                                .orderBy(desc("fraud_count")))
    
    (day_fraud_by_city_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/fraud_by_city/day"))
    
    # === Theo tháng ===
    # Phân tích fraud theo giờ
    month_fraud_by_hour_stats = (df_fraud_month_latest.groupBy("hour")
                                .agg(Fcount("*").alias("fraud_count"))
                                .orderBy(desc("fraud_count")))

    (month_fraud_by_hour_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/fraud_by_hour/month"))

    # Phân tích fraud theo merchant
    month_fraud_by_merchant_stats = (df_fraud_month_latest.groupBy("merchant_name", "merchant_city")
                                    .agg(Fcount("*").alias("fraud_count"))
                                    .orderBy(desc("fraud_count")))

    (month_fraud_by_merchant_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/fraud_by_merchant/month"))

    # Phân tích fraud theo city
    month_fraud_by_city_stats = (df_fraud_month_latest.groupBy("merchant_city", "merchant_state")
                                .agg(Fcount("*").alias("fraud_count"))
                                .orderBy(desc("fraud_count")))

    (month_fraud_by_city_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/fraud_by_city/month"))
    
    # === Theo năm ===
    # Phân tích fraud theo giờ
    year_fraud_by_hour_stats = (df_fraud_year_latest.groupBy("hour")
                                .agg(Fcount("*").alias("fraud_count"))
                                .orderBy(desc("fraud_count")))
    
    (year_fraud_by_hour_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/fraud_by_hour/year"))

    # Phân tích fraud theo merchant
    year_fraud_by_merchant_stats = (df_fraud_year_latest.groupBy("merchant_name", "merchant_city")
                                    .agg(Fcount("*").alias("fraud_count"))
                                    .orderBy(desc("fraud_count")))
    
    (year_fraud_by_merchant_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/fraud_by_merchant/year"))

    # Phân tích fraud theo city
    year_fraud_by_city_stats = (df_fraud_year_latest.groupBy("merchant_city", "merchant_state")
                                .agg(Fcount("*").alias("fraud_count"))
                                .orderBy(desc("fraud_count")))
    
    (year_fraud_by_city_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/fraud_by_city/year"))

    # Tính tỷ lệ fraud cho mỗi merchant
    merchant_total = df.groupBy("ds", "merchant_name").agg(Fcount("*").alias("total_transactions"))
    merchant_fraud = df.filter(col("is_fraud") == "Yes").groupBy("ds", "merchant_name").agg(Fcount("*").alias("fraud_transactions"))
    
    fraud_rate_by_merchant = (merchant_total.join(merchant_fraud, ["ds", "merchant_name"], "left")
                              .fillna(0, subset=["fraud_transactions"])
                              .withColumn("fraud_rate_percent", 
                                         Fround((col("fraud_transactions") / col("total_transactions")) * 100, 2))
                              .orderBy(desc("fraud_rate_percent")))
    
    (fraud_rate_by_merchant.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/fraud_rate_by_merchant"))
    
    print("[V] Phân tích fraud (theo giờ, city, merchant) - Hoàn thành")
    
    # ============================================================
    # YÊU CẦU 8: Tổng quan SUMMARY - Cho Power BI Dashboard
    # ============================================================
    daily_summary = (df.groupBy("ds")
                     .agg(
                         Fcount("*").alias("total_transactions"),
                         Fsum("amount_vnd").alias("total_revenue_vnd"),
                         Favg("amount_vnd").alias("avg_transaction_vnd"),
                         countDistinct("user_id").alias("unique_users"),
                         countDistinct("merchant_name").alias("unique_merchants"),
                         countDistinct("merchant_city").alias("unique_cities"),
                         Fsum(when(col("has_errors") != "", 1).otherwise(0)).alias("error_transactions"),
                         Fsum(when(col("is_fraud") == "Yes", 1).otherwise(0)).alias("fraud_transactions"),
                         Fmax("amount_vnd").alias("max_transaction_vnd"),
                         Fmin("amount_vnd").alias("min_transaction_vnd")
                     ))
    
    (daily_summary.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/daily_summary"))
    
    print("[V] Tổng quan hàng ngày - Hoàn thành")
    
    print("\n" + "="*60)
    print("[SUCCESS] Đã hoàn thành tất cả các phân tích!")
    print("="*60)
    print("Các dataset đã được lưu vào:")
    print(f"  1. {HDFS_WAREHOUSE}/hourly_analysis/(day, month, year)")
    print(f"  2. {HDFS_WAREHOUSE}/hourly_unusual_analysis/(day, month, year)")
    print(f"  3. {HDFS_WAREHOUSE}/city_analysis/(day, month, year)")
    print(f"  4. {HDFS_WAREHOUSE}/merchant_analysis/(day, month, year)")
    print(f"  5. {HDFS_WAREHOUSE}/user_analysis")
    print(f"  6. {HDFS_WAREHOUSE}/high_value_analysis/(day, month, year)")
    print(f"  7. {HDFS_WAREHOUSE}/weekday_vs_weekend")
    print(f"  8. {HDFS_WAREHOUSE}/user_error_analysis/(day, month, year)")
    print(f"  9. {HDFS_WAREHOUSE}/user_fraud_analysis/(day, month, year)")
    print(f" 10. {HDFS_WAREHOUSE}/fraud_by_hour/(day, month, year)")
    print(f" 11. {HDFS_WAREHOUSE}/fraud_by_merchant/(day, month, year)")
    print(f" 12. {HDFS_WAREHOUSE}/fraud_by_city/(day, month, year)")
    print(f" 13. {HDFS_WAREHOUSE}/fraud_rate_by_merchant")
    print(f" 14. {HDFS_WAREHOUSE}/daily_summary")

if __name__ == "__main__":
    main()