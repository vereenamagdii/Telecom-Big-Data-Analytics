#!/usr/bin/env python3
"""
Telecom Big Data Analytics Platform - Real-time Anomaly Detection
Streams CDR events from Kafka, parses incoming records, and flags abnormal usage
(such as Customer 10231 with >= 25 GB/hour usage).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

def main():
    # Initialize Spark Session with Kafka support
    spark = SparkSession.builder \
        .appName("TelecomRealTimeAnomalyDetector") \
        .getOrCreate()

    # Set logging level to reduce console noise
    spark.sparkContext.setLogLevel("WARN")

    # Define incoming JSON schema matching Kafka producer messages
    schema = StructType() \
        .add("Customer_ID", StringType()) \
        .add("Date", StringType()) \
        .add("Time", StringType()) \
        .add("Cell_ID", StringType()) \
        .add("msisdn", StringType()) \
        .add("Call_Duration", IntegerType()) \
        .add("Data_Usage", DoubleType()) \
        .add("SMS_Count", IntegerType()) \
        .add("Recharge_Amount", DoubleType()) \
        .add("Package", StringType())

    # 1. Read Stream from Kafka topic 'telecom-cdr'
    kafka_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "telecom-cdr") \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse JSON payload
    parsed_df = kafka_raw.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # 3. Anomaly Rule: Flag high data consumption (e.g. >= 25.0 GB usage spike)
    anomalies_df = parsed_df.filter(col("Data_Usage") >= 25.0)

    # 4. Write Real-Time Alerts to Console (Console Sink)
    query = anomalies_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    print("=== Spark Real-Time Anomaly Detector Active ===")
    query.awaitTermination()

if __name__ == "__main__":
    main()
