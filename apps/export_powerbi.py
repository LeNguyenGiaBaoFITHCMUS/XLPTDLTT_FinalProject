from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as Fmax

HDFS_STATS = "hdfs://namenode:9000/warehouse/daily_merchant_stats"
HDFS_POWERBI = "hdfs://namenode:9000/powerbi/daily_merchant_stats_latest"

def main():
    spark = SparkSession.builder.appName("ExportPowerBI").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("header", "true").csv(HDFS_STATS)

    latest_ds = df.select(Fmax(col("ds")).alias("ds")).collect()[0]["ds"]
    latest = df.where(col("ds") == latest_ds)

    (latest.coalesce(1).write
        .mode("overwrite")
        .option("header", "true")
        .csv(HDFS_POWERBI)
    )

    print("[ExportPowerBI] latest ds:", latest_ds)
    print("[ExportPowerBI] wrote to:", HDFS_POWERBI)

if __name__ == "__main__":
    main()
