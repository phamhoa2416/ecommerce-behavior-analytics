import csv
import json
import os

from kafka import KafkaProducer

CSV_FILE_PATH = os.getenv("CSV_FILE_PATH_KAFKA", "data/2019-Nov.csv")
KAFKA_STREAM_TOPIC = os.getenv("KAFKA_STREAM_TOPIC", "ecommerce_events")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")

def process_to_kafka():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        linger_ms=20,
        batch_size=65536,
        buffer_memory=33554432,
        compression_type="snappy",
        acks="all",
        retries=3,
        max_in_flight_requests_per_connection=1,
        enable_idempotence=True,
    )

    print(f"Starting stream from {CSV_FILE_PATH} to topic '{KAFKA_STREAM_TOPIC}'...")

    try:
        with open(CSV_FILE_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            total_messages = 0

            for row in reader:
                producer.send(
                    KAFKA_STREAM_TOPIC,
                    value=json.dumps(row).encode("utf-8"),
                )
                total_messages += 1

            producer.flush()
            print(f"\nTotal messages sent: {total_messages}")

    except FileNotFoundError:
        print(f"Error: File not found at {CSV_FILE_PATH}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    process_to_kafka()