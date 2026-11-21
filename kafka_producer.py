#!/usr/bin/env python3
import time
import csv
import json
import os
from kafka import KafkaProducer

# Initialize Kafka Producer targeting KRaft broker
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Resolve path: try sample data first, then fall back to root/HDFS data
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_FILE = os.path.join(base_dir, 'data_sample', 'telecom_cdr.csv')

if not os.path.exists(CSV_FILE):
    CSV_FILE = os.path.join(base_dir, 'data', 'telecom_cdr.csv')

print(f"Reading dataset from: {CSV_FILE}")

try:
    with open(CSV_FILE, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        count = 0
        for row in reader:
            # Type casting for numeric fields
            if 'Data_Usage' in row and row['Data_Usage']:
                row['Data_Usage'] = float(row['Data_Usage'])
            if 'Call_Duration' in row and row['Call_Duration']:
                row['Call_Duration'] = int(row['Call_Duration'])
            if 'SMS_Count' in row and row['SMS_Count']:
                row['SMS_Count'] = int(row['SMS_Count'])
            if 'Recharge_Amount' in row and row['Recharge_Amount']:
                row['Recharge_Amount'] = float(row['Recharge_Amount'])

            # Send record to Kafka topic
            producer.send('telecom-cdr', value=row)
            count += 1

            if count % 500 == 0:
                print(f"Sent {count} events...")

            time.sleep(0.01) # Simulates real-time streaming pace

        producer.flush()
        print(f"Successfully streamed {count} records to Kafka!")

except Exception as e:
    print(f"Error reading or streaming dataset: {e}")
