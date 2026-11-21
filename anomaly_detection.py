from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    hour,
    sum as spark_sum,
    max as spark_max,
    count,
    when,
    lit
)

spark = SparkSession.builder \
    .appName("TelecomAnomalyDetection") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

INPUT = "hdfs:///telecom/processed/clean_cdr"
OUTPUT = "hdfs:///telecom/analytics/anomalies"

# --------------------------------------------------
# Read cleaned CDR data
# --------------------------------------------------
df = spark.read.parquet(INPUT)

# Extract hour from the Time timestamp
df = df.withColumn("Hour", hour(col("Time")))

# --------------------------------------------------
# 1. Hourly statistics per customer
# --------------------------------------------------
hourly = df.groupBy(
    "Customer_ID",
    "Date",
    "Hour"
).agg(
    spark_sum("Data_Usage").alias("Total_Data_MB"),
    spark_max("Call_Duration").alias("Max_Call_Duration"),
    spark_sum("SMS_Count").alias("Total_SMS")
)

# --------------------------------------------------
# 2. Detect hourly anomalies
# --------------------------------------------------
hourly_anomalies = hourly.filter(
    (col("Total_Data_MB") >= 20000) |
    (col("Max_Call_Duration") >= 16000) |
    (col("Total_SMS") >= 300)
)

hourly_anomalies = hourly_anomalies.withColumn(
    "Anomaly_Type",
    when(
        col("Total_Data_MB") >= 20000,
        "DATA_SPIKE"
    ).when(
        col("Max_Call_Duration") >= 16000,
        "LONG_CALL"
    ).when(
        col("Total_SMS") >= 300,
        "SMS_SPAM"
    )
)

# --------------------------------------------------
# 3. Recharge burst detection
# --------------------------------------------------
# Count recharge events per customer per day
recharge_daily = df.filter(
    col("Recharge_Amount") > 0
).groupBy(
    "Customer_ID",
    "Date"
).agg(
    count("*").alias("Recharge_Count")
)

# Customers with 10 or more recharge events in one day
recharge_anomalies = recharge_daily.filter(
    col("Recharge_Count") >= 10
)

# Find the hour with the highest number of recharge events
recharge_hourly = df.filter(
    col("Recharge_Amount") > 0
).groupBy(
    "Customer_ID",
    "Date",
    "Hour"
).agg(
    count("*").alias("Hourly_Recharge_Count")
)

recharge_hour_max = recharge_hourly.groupBy(
    "Customer_ID",
    "Date"
).agg(
    spark_max("Hourly_Recharge_Count").alias("Max_Hourly_Recharge")
)

recharge_anomalies = recharge_anomalies.join(
    recharge_hourly,
    ["Customer_ID", "Date"],
    "left"
).join(
    recharge_hour_max,
    ["Customer_ID", "Date"],
    "left"
).filter(
    col("Hourly_Recharge_Count") == col("Max_Hourly_Recharge")
).select(
    "Customer_ID",
    "Date",
    "Hour"
).withColumn(
    "Total_Data_MB",
    lit(0.0)
).withColumn(
    "Max_Call_Duration",
    lit(0)
).withColumn(
    "Total_SMS",
    lit(0)
).withColumn(
    "Anomaly_Type",
    lit("RECHARGE_BURST")
)

# --------------------------------------------------
# 4. Combine all detected anomalies
# --------------------------------------------------
result = hourly_anomalies.select(
    "Customer_ID",
    "Date",
    "Hour",
    "Total_Data_MB",
    "Max_Call_Duration",
    "Total_SMS",
    "Anomaly_Type"
).unionByName(
    recharge_anomalies.select(
        "Customer_ID",
        "Date",
        "Hour",
        "Total_Data_MB",
        "Max_Call_Duration",
        "Total_SMS",
        "Anomaly_Type"
    )
)

# --------------------------------------------------
# 5. Save anomaly results
# --------------------------------------------------
result.write \
    .mode("overwrite") \
    .parquet(OUTPUT)

# --------------------------------------------------
# 6. Show summary
# --------------------------------------------------
print("==========================================")
print("Telecom Anomaly Detection Completed")
print("Detected anomalies:", result.count())
print("Output:", OUTPUT)
print("==========================================")

result.orderBy(
    "Date",
    "Hour",
    "Customer_ID"
).show(50, truncate=False)

spark.stop()
