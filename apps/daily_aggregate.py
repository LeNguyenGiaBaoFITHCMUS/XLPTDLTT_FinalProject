from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as Fsum, count as Fcount

HDFS_CLEAN = "hdfs://namenode:9000/datalake/transactions_clean"
HDFS_OUT = "hdfs://namenode:9000/warehouse/daily_merchant_stats"

def main():
    spark = SparkSession.builder.appName("DailyAggregate").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(HDFS_CLEAN)

    # df includes ds partition column
    agg = (df.groupBy("ds", "merchant_name")
           .agg(
               Fcount("*").alias("transaction_count"),
               Fsum(col("amount_vnd")).alias("total_amount_vnd")
           )
          )

    # output partitioned by ds so PowerBI can filter by date
    (agg.write
        .mode("append")
        .option("header", "true")
        .csv(HDFS_OUT)
    )

    print("[DailyAggregate] wrote to:", HDFS_OUT)

if __name__ == "__main__":
    main()
