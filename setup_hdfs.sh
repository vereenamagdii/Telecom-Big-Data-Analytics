#!/bin/bash

echo "======================================"
echo " Telecom Big Data - HDFS Setup"
echo "======================================"

echo ""
echo "Creating HDFS directories..."

hdfs dfs -mkdir -p /telecom/raw/telecom_data
hdfs dfs -mkdir -p /telecom/processed/telecom_data
hdfs dfs -mkdir -p /telecom/output/usage
hdfs dfs -mkdir -p /telecom/output/peak_hours
hdfs dfs -mkdir -p /telecom/output/suspicious_usage

echo ""
echo "HDFS directory structure:"
echo "--------------------------------------"

hdfs dfs -ls -R /telecom

echo ""
echo "======================================"
echo " HDFS setup completed successfully!"
echo "======================================"
