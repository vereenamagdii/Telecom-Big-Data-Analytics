from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, hour, when

spark = SparkSession.builder \
    .appName("Telecom Batch Analytics") \
    .getOrCreate()

# ==========================================
# PATHS
# ==========================================

input_path = "hdfs:///telecom/processed/clean_cdr"
analytics_path = "hdfs:///telecom/analytics"

# ==========================================
# 1. READ CLEAN DATA
# ==========================================

print("===== READING CLEAN DATA =====")

df = spark.read.parquet(input_path)

print("Total rows:", df.count())

print("===== CLEAN DATA SCHEMA =====")
df.printSchema()


# ==========================================
# 2. TOP CUSTOMERS BY DATA USAGE
# ==========================================

print("===== TOP CUSTOMERS BY DATA USAGE =====")

top_customers = df.groupBy("Customer_ID") \
    .agg(
        sum("Data_Usage").alias("Total_Data_Usage_MB"),
        sum("Call_Duration").alias("Total_Call_Duration_Seconds"),
        sum("SMS_Count").alias("Total_SMS"),
        sum("Recharge_Amount").alias("Total_Recharge"),
        count("*").alias("Number_of_Records")
    ) \
    .orderBy(col("Total_Data_Usage_MB").desc())

top_customers.show(10, truncate=False)

top_customers.write \
    .mode("overwrite") \
    .parquet(analytics_path + "/top_customers")

print("Saved: /telecom/analytics/top_customers")


# ==========================================
# 3. MOST ACTIVE CELLS
# ==========================================

print("===== MOST ACTIVE CELLS =====")

active_cells = df.groupBy("Cell_ID") \
    .agg(
        count("*").alias("Number_of_Records"),
        sum("Data_Usage").alias("Total_Data_Usage_MB"),
        sum("Call_Duration").alias("Total_Call_Duration_Seconds")
    ) \
    .orderBy(col("Number_of_Records").desc())

active_cells.show(10, truncate=False)

active_cells.write \
    .mode("overwrite") \
    .parquet(analytics_path + "/active_cells")

print("Saved: /telecom/analytics/active_cells")


# ==========================================
# 4. PEAK HOURS
# ==========================================

print("===== PEAK HOURS =====")

peak_hours = df.withColumn(
    "Hour",
    hour("Time")
).groupBy("Hour") \
    .agg(
        count("*").alias("Number_of_Records"),
        sum("Data_Usage").alias("Total_Data_Usage_MB"),
        avg("Call_Duration").alias("Average_Call_Duration_Seconds")
    ) \
    .orderBy(col("Number_of_Records").desc())

peak_hours.show(24, truncate=False)

peak_hours.write \
    .mode("overwrite") \
    .parquet(analytics_path + "/peak_hours")

print("Saved: /telecom/analytics/peak_hours")


# ==========================================
# 5. AVERAGE CALL DURATION
# ==========================================

print("===== AVERAGE CALL DURATION =====")

average_call = df.agg(
    avg("Call_Duration").alias("Average_Call_Duration_Seconds")
)

average_call.show(truncate=False)

average_call.write \
    .mode("overwrite") \
    .parquet(analytics_path + "/call_duration")

print("Saved: /telecom/analytics/call_duration")


# ==========================================
# 6. PACKAGE ANALYSIS
# ==========================================

print("===== PACKAGE ANALYSIS =====")

package_analysis = df.groupBy("Package") \
    .agg(
        count("*").alias("Number_of_Records"),
        sum("Data_Usage").alias("Total_Data_Usage_MB"),
        sum("Recharge_Amount").alias("Total_Revenue_EGP"),
        avg("Call_Duration").alias("Average_Call_Duration_Seconds")
    ) \
    .orderBy(col("Total_Revenue_EGP").desc())

package_analysis.show(truncate=False)

package_analysis.write \
    .mode("overwrite") \
    .parquet(analytics_path + "/package_analysis")

print("Saved: /telecom/analytics/package_analysis")


# ==========================================
# 7. CUSTOMER SEGMENTATION
# ==========================================

print("===== CUSTOMER SEGMENTATION =====")

customer_usage = df.groupBy("Customer_ID") \
    .agg(
        sum("Data_Usage").alias("Total_Data_Usage_MB"),
        sum("Call_Duration").alias("Total_Call_Duration_Seconds"),
        sum("SMS_Count").alias("Total_SMS"),
        sum("Recharge_Amount").alias("Total_Recharge")
    )

quantiles = customer_usage.approxQuantile(
    "Total_Data_Usage_MB",
    [0.33, 0.66],
    0.01
)

low_medium_threshold = quantiles[0]
medium_high_threshold = quantiles[1]

print("Low/Medium threshold:", low_medium_threshold)
print("Medium/High threshold:", medium_high_threshold)

segmentation = customer_usage.withColumn(
    "Customer_Segment",
    when(
        col("Total_Data_Usage_MB") <= low_medium_threshold,
        "Low Usage"
    ).when(
        col("Total_Data_Usage_MB") <= medium_high_threshold,
        "Medium Usage"
    ).otherwise(
        "High Usage"
    )
)

segmentation.show(20, truncate=False)

segmentation.write \
    .mode("overwrite") \
    .parquet(analytics_path + "/segmentation")

print("Saved: /telecom/analytics/segmentation")


# ==========================================
# END
# ==========================================

print("===== ALL ANALYTICS SAVED SUCCESSFULLY =====")

spark.stop()
