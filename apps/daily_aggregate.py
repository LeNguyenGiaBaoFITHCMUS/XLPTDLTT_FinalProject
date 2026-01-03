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
    
    hourly_stats = (df_with_hour.groupBy("ds", "hour")
                    .agg(
                        Fcount("*").alias("transaction_count"),
                        Fsum("amount_vnd").alias("total_amount_vnd"),
                        Favg("amount_vnd").alias("avg_amount_vnd")
                    )
                    .orderBy("ds", "hour"))
    
    (hourly_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/hourly_analysis"))
    
    print("[V] Phân tích theo giờ - Hoàn thành")
    
    # ============================================================
    # YÊU CẦU 2: Phân tích theo THÀNH PHỐ - Tìm thành phố có giá trị cao nhất
    # ============================================================
    city_stats = (df.groupBy("ds", "merchant_city", "merchant_state")
                  .agg(
                      Fcount("*").alias("transaction_count"),
                      Fsum("amount_vnd").alias("total_amount_vnd"),
                      Favg("amount_vnd").alias("avg_amount_vnd"),
                      countDistinct("user_id").alias("unique_users"),
                      countDistinct("merchant_name").alias("unique_merchants")
                  )
                  .orderBy(desc("total_amount_vnd")))
    
    (city_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/city_analysis"))
    
    print("[V] Phân tích theo thành phố - Hoàn thành")
    
    # ============================================================
    # YÊU CẦU 3: Phân tích theo MERCHANT - Số lượng và giá trị giao dịch
    # ============================================================
    merchant_stats = (df.groupBy("ds", "merchant_name", "merchant_city", "mcc")
                      .agg(
                          Fcount("*").alias("transaction_count"),
                          Fsum("amount_vnd").alias("total_amount_vnd"),
                          Favg("amount_vnd").alias("avg_amount_vnd"),
                          Fmax("amount_vnd").alias("max_amount_vnd"),
                          countDistinct("user_id").alias("unique_customers")
                      )
                      .orderBy(desc("total_amount_vnd")))
    
    (merchant_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/merchant_analysis"))
    
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
                      Fsum(when(col("has_errors") == "Yes", 1).otherwise(0)).alias("error_count")
                  )
                  .orderBy(desc("transaction_count")))
    
    (user_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/user_analysis"))
    
    print("[V] Phân tích user behavior - Hoàn thành")
    
    # ============================================================
    # YÊU CẦU 5: Giao dịch GIÁ TRỊ LỚN - Tìm pattern của giao dịch lớn
    # ============================================================
    quantiles = df.stat.approxQuantile("amount_vnd", [0.95], 0.01)
    high_value_threshold = quantiles[0] if quantiles else 0
    
    df_high_value_with_hour = df.withColumn("hour", hour(col("time")))
    
    high_value_transactions = (df_high_value_with_hour.filter(col("amount_vnd") >= high_value_threshold)
                               .groupBy("ds", "hour", "merchant_city", "merchant_name")
                               .agg(
                                   Fcount("*").alias("high_value_count"),
                                   Favg("amount_vnd").alias("avg_high_value_vnd"),
                                   Fmax("amount_vnd").alias("max_amount_vnd")
                               )
                               .orderBy(desc("high_value_count")))
    
    (high_value_transactions.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/high_value_analysis"))
    
    print(f"[V] Phân tích giao dịch giá trị lớn (>= {high_value_threshold:,.0f} VND) - Hoàn thành")
    
    # ============================================================
    # YÊU CẦU 6: So sánh NGÀY THƯỜNG vs CUỐI TUẦN
    # ============================================================
    df_with_dayofweek = df.withColumn("day_of_week", dayofweek(col("transaction_date")))
    df_with_daytype = df_with_dayofweek.withColumn(
        "day_type",
        when((col("day_of_week") == 1) | (col("day_of_week") == 7), "Weekend")
        .otherwise("Weekday")
    )
    
    weekday_comparison = (df_with_daytype.groupBy("ds", "day_type")
                          .agg(
                              Fcount("*").alias("transaction_count"),
                              Fsum("amount_vnd").alias("total_amount_vnd"),
                              Favg("amount_vnd").alias("avg_amount_vnd"),
                              countDistinct("user_id").alias("unique_users")
                          ))
    
    (weekday_comparison.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/weekday_vs_weekend"))
    
    print("[V] So sánh ngày thường vs cuối tuần - Hoàn thành")
    
    # ============================================================
    # YÊU CẦU 7: Phân tích LỖI - User có lỗi nhiều bất thường
    # ============================================================
    error_stats = (df.filter(col("has_errors") == "Yes")
                   .groupBy("ds", "user_id", "merchant_name")
                   .agg(
                       Fcount("*").alias("error_count"),
                       Fsum("amount_vnd").alias("total_error_amount_vnd")
                   )
                   .orderBy(desc("error_count")))
    
    (error_stats.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/error_analysis"))
    
    print("[V] Phân tích lỗi giao dịch - Hoàn thành")
    
    # ============================================================
    # YÊU CẦU 4 & 7: Phân tích FRAUD - Xu hướng fraud theo giờ, city, merchant
    # ============================================================
    df_fraud_with_hour = df.withColumn("hour", hour(col("time")))
    
    # Phân tích fraud theo giờ
    fraud_by_hour = (df_fraud_with_hour.filter(col("is_fraud") == "Yes")
                     .groupBy("ds", "hour")
                     .agg(
                         Fcount("*").alias("fraud_count"),
                         Fsum("amount_vnd").alias("total_fraud_amount_vnd"),
                         Favg("amount_vnd").alias("avg_fraud_amount_vnd")
                     ))
    
    # Phân tích fraud theo city
    fraud_by_city = (df.filter(col("is_fraud") == "Yes")
                     .groupBy("ds", "merchant_city", "merchant_state")
                     .agg(
                         Fcount("*").alias("fraud_count"),
                         Fsum("amount_vnd").alias("total_fraud_amount_vnd")
                     ))
    
    # Phân tích fraud theo merchant
    fraud_by_merchant = (df.filter(col("is_fraud") == "Yes")
                         .groupBy("ds", "merchant_name", "merchant_city")
                         .agg(
                             Fcount("*").alias("fraud_count"),
                             Fsum("amount_vnd").alias("total_fraud_amount_vnd"),
                             countDistinct("user_id").alias("unique_fraud_users")
                         ))
    
    # Tính tỷ lệ fraud cho mỗi city
    city_total = df.groupBy("ds", "merchant_city").agg(Fcount("*").alias("total_transactions"))
    city_fraud = df.filter(col("is_fraud") == "Yes").groupBy("ds", "merchant_city").agg(Fcount("*").alias("fraud_transactions"))
    
    fraud_rate_by_city = (city_total.join(city_fraud, ["ds", "merchant_city"], "left")
                          .fillna(0, subset=["fraud_transactions"])
                          .withColumn("fraud_rate_percent", 
                                     Fround((col("fraud_transactions") / col("total_transactions")) * 100, 2))
                          .orderBy(desc("fraud_rate_percent")))
    
    # Tính tỷ lệ fraud cho mỗi merchant
    merchant_total = df.groupBy("ds", "merchant_name").agg(Fcount("*").alias("total_transactions"))
    merchant_fraud = df.filter(col("is_fraud") == "Yes").groupBy("ds", "merchant_name").agg(Fcount("*").alias("fraud_transactions"))
    
    fraud_rate_by_merchant = (merchant_total.join(merchant_fraud, ["ds", "merchant_name"], "left")
                              .fillna(0, subset=["fraud_transactions"])
                              .withColumn("fraud_rate_percent", 
                                         Fround((col("fraud_transactions") / col("total_transactions")) * 100, 2))
                              .orderBy(desc("fraud_rate_percent")))
    
    # Lưu phân tích fraud
    (fraud_rate_by_city.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/fraud_by_city"))
    
    (fraud_rate_by_merchant.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/fraud_by_merchant"))
    
    (fraud_by_hour.write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{HDFS_WAREHOUSE}/fraud_by_hour"))
    
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
                         Fsum(when(col("has_errors") == "Yes", 1).otherwise(0)).alias("error_transactions"),
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
    print(f"  1. {HDFS_WAREHOUSE}/hourly_analysis")
    print(f"  2. {HDFS_WAREHOUSE}/city_analysis")
    print(f"  3. {HDFS_WAREHOUSE}/merchant_analysis")
    print(f"  4. {HDFS_WAREHOUSE}/user_analysis")
    print(f"  5. {HDFS_WAREHOUSE}/high_value_analysis (có hour)")
    print(f"  6. {HDFS_WAREHOUSE}/weekday_vs_weekend")
    print(f"  7. {HDFS_WAREHOUSE}/error_analysis")
    print(f"  8. {HDFS_WAREHOUSE}/daily_summary (có fraud count)")
    print(f"  9. {HDFS_WAREHOUSE}/fraud_by_hour")
    print(f" 10. {HDFS_WAREHOUSE}/fraud_by_city")
    print(f" 11. {HDFS_WAREHOUSE}/fraud_by_merchant")

if __name__ == "__main__":
    main()
