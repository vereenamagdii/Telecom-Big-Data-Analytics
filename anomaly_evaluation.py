from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ==========================================
# Start Spark
# ==========================================

spark = SparkSession.builder \
    .appName("TelecomAnomalyEvaluation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ==========================================
# 1. Read Ground Truth
#    IMPORTANT:
#    Ground truth is stored locally
# ==========================================

truth = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(
        "file:///home/dell/telecom-big-data-project/data/anomalies_truth.csv"
    )


# ==========================================
# 2. Read Detected Anomalies from HDFS
# ==========================================

detected = spark.read.parquet(
    "hdfs:///telecom/analytics/anomalies"
)


# ==========================================
# 3. Make comparison columns the same type
# ==========================================

keys = [
    "Customer_ID",
    "Date",
    "Hour",
    "Anomaly_Type"
]

for c in keys:
    truth = truth.withColumn(
        c,
        col(c).cast("string")
    )

    detected = detected.withColumn(
        c,
        col(c).cast("string")
    )


# ==========================================
# 4. Remove duplicate records
# ==========================================

truth = truth.select(keys).dropDuplicates()

detected = detected.select(keys).dropDuplicates()


# ==========================================
# 5. Calculate True Positives
# ==========================================

true_positives = truth.join(
    detected,
    keys,
    "inner"
).count()


# ==========================================
# 6. Calculate False Positives
# ==========================================

false_positives = detected.join(
    truth,
    keys,
    "left_anti"
).count()


# ==========================================
# 7. Calculate False Negatives
# ==========================================

false_negatives = truth.join(
    detected,
    keys,
    "left_anti"
).count()


# ==========================================
# 8. Calculate Precision
# ==========================================

if true_positives + false_positives > 0:

    precision = true_positives / (
        true_positives + false_positives
    )

else:

    precision = 0.0


# ==========================================
# 9. Calculate Recall
# ==========================================

if true_positives + false_negatives > 0:

    recall = true_positives / (
        true_positives + false_negatives
    )

else:

    recall = 0.0


# ==========================================
# 10. Calculate F1 Score
# ==========================================

if precision + recall > 0:

    f1 = 2 * precision * recall / (
        precision + recall
    )

else:

    f1 = 0.0


# ==========================================
# 11. Print Evaluation Results
# ==========================================

print()
print("==========================================")
print("Telecom Anomaly Detection Evaluation")
print("==========================================")

print("Ground Truth Anomalies :", truth.count())
print("Detected Anomalies     :", detected.count())

print("------------------------------------------")

print("True Positives (TP)    :", true_positives)
print("False Positives (FP)   :", false_positives)
print("False Negatives (FN)   :", false_negatives)

print("------------------------------------------")

print("Precision              :", round(precision, 4))
print("Recall                 :", round(recall, 4))
print("F1 Score               :", round(f1, 4))

print("==========================================")
print()


# ==========================================
# Stop Spark
# ==========================================

spark.stop()
