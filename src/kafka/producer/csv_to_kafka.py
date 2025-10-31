import json
import time

import pandas as pd
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'ecommerce_data'

def setup_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=3,
        batch_size=16384,
        linger_ms=10,
        compression_type='gzip'
    )
    return producer

def csv_to_kafka(csv_file, delay=1.0):
    df = pd.read_csv(csv_file)
    producer = setup_producer()
    
    print(f"Starting to send data from {csv_file} to Kafka topic {TOPIC_NAME}...")
    
    for index, row in df.iterrows():
        record = row.to_dict()
        producer.send(TOPIC_NAME, value=record)
        print(f"Sent record: {record}")
        time.sleep(delay)

if __name__ == "__main__":
    csv_file_path = '2019-Oct.csv'
    csv_to_kafka(csv_file_path, 1.0)