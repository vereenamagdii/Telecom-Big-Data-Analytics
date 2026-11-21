import csv
import json
import time
from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC = "telecom-data"
CSV_FILE = "data/telecom_cdr.csv"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

with open(CSV_FILE, "r", encoding="utf-8") as file:
    reader = csv.DictReader(file)

    for row in reader:
        producer.send(TOPIC, value=row)
        time.sleep(0.5)

producer.flush()
producer.close()

print("All telecom records sent successfully!")
