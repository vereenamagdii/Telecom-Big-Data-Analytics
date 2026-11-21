from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, expr

# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("Telecom CDR Data Cleaning") \
    .getOrCreate()

# ============================================================
# 2. PATHS
# ============================================================

input_path = "hdfs:///telecom/raw/cdr/telecom_cdr.csv"
output_path = "hdfs:///telecom/processed/clean_cdr"

# ============================================================
# 3. READ RAW CDR DATA
# ============================================================

print("===== READING RAW CDR DATA =====")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

print("Total rows before cleaning:", df.count())

print("===== RAW SCHEMA =====")
df.printSchema()

# ============================================================
# 4. STANDARDIZE DATE
# ============================================================

print("===== CLEANING DATE =====")

cleanedDF = df.withColumn(
    "Date",
    when(
        expr("try_to_timestamp(Date, 'yyyy-MM-dd')").isNotNull(),
        to_date(col("Date"), "yyyy-MM-dd")
    ).otherwise(
        to_date(col("Date"), "dd/MM/yyyy")
    )
)

# ============================================================
# 5. REMOVE DUPLICATES
# ============================================================

print("===== REMOVING DUPLICATES =====")

before_duplicates = cleanedDF.count()

cleanedDF = cleanedDF.dropDuplicates()

after_duplicates = cleanedDF.count()

print("Rows before duplicate removal:", before_duplicates)
print("Rows after duplicate removal:", after_duplicates)
print("Duplicates removed:", before_duplicates - after_duplicates)

# ============================================================
# 6. REMOVE NEGATIVE DATA USAGE
# ============================================================

print("===== REMOVING INVALID DATA USAGE =====")

before_negative = cleanedDF.count()

cleanedDF = cleanedDF.withColumn(
    "Data_Usage",
    when(col("Data_Usage") < 0, 0.0).otherwise(col("Data_Usage"))
)

after_negative = cleanedDF.count()

print("Rows before negative-value removal:", before_negative)
print("Rows after negative-value removal:", after_negative)
print("Negative rows removed:", before_negative - after_negative)

# ============================================================
# 7. REMOVE ROWS WITH CRITICAL NULLS
# ============================================================

print("===== HANDLING CRITICAL NULLS =====")

before_nulls = cleanedDF.count()

cleanedDF = cleanedDF.filter(
    col("Customer_ID").isNotNull() &
    col("Cell_ID").isNotNull()
)

after_nulls = cleanedDF.count()

print("Rows before critical-null removal:", before_nulls)
print("Rows after critical-null removal:", after_nulls)
print("Rows removed بسبب missing Customer_ID/Cell_ID:",
      before_nulls - after_nulls)

# ============================================================
# 8. CHECK FINAL DATA
# ============================================================

print("===== FINAL DATA =====")

print("Final rows:", cleanedDF.count())

print("===== FINAL SCHEMA =====")
cleanedDF.printSchema()

print("===== FINAL SAMPLE =====")
cleanedDF.show(10, truncate=False)

# ============================================================
# 9. SAVE CLEAN DATA TO HDFS
# ============================================================

print("===== SAVING CLEAN DATA TO HDFS =====")

cleanedDF.write \
    .mode("overwrite") \
    .parquet(output_path)

print("Clean data saved successfully to:")
print(output_path)

# ============================================================
# 10. STOP SPARK
# ============================================================

spark.stop()
