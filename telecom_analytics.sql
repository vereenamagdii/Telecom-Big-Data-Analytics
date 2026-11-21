-- =========================================================
-- Telecom Big Data Analytics Platform
-- Hive Analytics - Nour
-- =========================================================

-- Use MapReduce execution engine
SET hive.execution.engine=mr;

-- Required library for Hive MapReduce jobs
ADD JAR /usr/local/hive/lib/commons-collections-3.2.2.jar;


-- =========================================================
-- 1. DATABASE
-- =========================================================

CREATE DATABASE IF NOT EXISTS telecom_project;

USE telecom_project;


-- =========================================================
-- 2. TELECOM CDR TABLE
-- =========================================================

CREATE EXTERNAL TABLE IF NOT EXISTS telecom_cdr (
    customer_id INT,
    record_date STRING,
    record_time STRING,
    cell_id STRING,
    msisdn STRING,
    call_duration INT,
    data_usage DOUBLE,
    sms_count INT,
    recharge_amount DOUBLE,
    package_name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/telecom/input'
TBLPROPERTIES ('skip.header.line.count'='1');


-- =========================================================
-- 3. DATA CHECK
-- =========================================================

SELECT COUNT(*) AS total_records
FROM telecom_cdr
WHERE customer_id IS NOT NULL;


-- =========================================================
-- 4. TOP CUSTOMERS BY DATA USAGE
-- =========================================================

SELECT
    customer_id,
    SUM(data_usage) AS total_data_usage
FROM telecom_cdr
WHERE customer_id IS NOT NULL
GROUP BY customer_id
ORDER BY total_data_usage DESC
LIMIT 10;


-- =========================================================
-- 5. MOST ACTIVE CELLS
-- =========================================================

SELECT
    cell_id,
    COUNT(*) AS activity_count
FROM telecom_cdr
WHERE customer_id IS NOT NULL
GROUP BY cell_id
ORDER BY activity_count DESC
LIMIT 10;


-- =========================================================
-- 6. PEAK HOURS
-- =========================================================

SELECT
    SUBSTR(record_time, 1, 2) AS hour,
    COUNT(*) AS total_activity
FROM telecom_cdr
WHERE customer_id IS NOT NULL
GROUP BY SUBSTR(record_time, 1, 2)
ORDER BY total_activity DESC;


-- =========================================================
-- 7. AVERAGE CALL DURATION
-- =========================================================

SELECT
    AVG(call_duration) AS average_call_duration
FROM telecom_cdr
WHERE customer_id IS NOT NULL;


-- =========================================================
-- 8. TOTAL DATA CONSUMPTION
-- =========================================================

SELECT
    SUM(data_usage) AS total_data_consumption
FROM telecom_cdr
WHERE customer_id IS NOT NULL;


-- =========================================================
-- 9. REVENUE PER PACKAGE
-- =========================================================

SELECT
    package_name,
    SUM(recharge_amount) AS total_revenue
FROM telecom_cdr
WHERE customer_id IS NOT NULL
GROUP BY package_name
ORDER BY total_revenue DESC;


-- =========================================================
-- 10. CUSTOMER SEGMENTATION
-- =========================================================

SELECT
    customer_id,
    SUM(data_usage) AS total_data_usage,
    CASE
        WHEN SUM(data_usage) >= 100 THEN 'High Usage'
        WHEN SUM(data_usage) >= 50 THEN 'Medium Usage'
        ELSE 'Low Usage'
    END AS customer_segment
FROM telecom_cdr
WHERE customer_id IS NOT NULL
GROUP BY customer_id
ORDER BY total_data_usage DESC;


-- =========================================================
-- 11. SUSPICIOUS USAGE
-- Demo threshold. Final threshold can follow Spark anomaly logic.
-- =========================================================

SELECT
    customer_id,
    record_date,
    record_time,
    data_usage
FROM telecom_cdr
WHERE customer_id IS NOT NULL
  AND data_usage >= 20
ORDER BY data_usage DESC;

