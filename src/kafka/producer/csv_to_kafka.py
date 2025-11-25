import json
import time
import logging
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "ecommerce_data"


def setup_producer():
    logger.info(f"Initializing Kafka Producer with broker: {KAFKA_BROKER}")

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=5,
            batch_size=16384,
            linger_ms=10,
            compression_type="gzip",
            request_timeout_ms=20000,
            max_block_ms=20000,
        )
        logger.info("Kafka Producer initialized successfully!")
        return producer
    except Exception as e:
        logger.error(f"Failed to initialize Kafka Producer: {e}")
        raise e


def send_with_log(producer, record, index):
    start_time = time.time()

    try:
        future = producer.send(TOPIC_NAME, value=record)

        # Callback for success
        def on_success(metadata):
            elapsed = (time.time() - start_time) * 1000
            logger.info(
                f"[OK] Sent message #{index} -> Topic: {metadata.topic}, "
                f"Partition: {metadata.partition}, Offset: {metadata.offset}, "
                f"Time: {elapsed:.2f} ms"
            )

        # Callback for failure
        def on_error(excp):
            logger.error(f"[ERR] Failed to send message #{index}: {excp}")

        future.add_callback(on_success)
        future.add_errback(on_error)

    except KafkaError as e:
        logger.error(f"[KAFKA ERROR] Message #{index} failed: {e}")


def csv_to_kafka(csv_file, delay=1.0):
    logger.info(f"Reading CSV file: {csv_file}")
    df = pd.read_csv(csv_file)

    logger.info(f"Loaded {len(df)} rows from CSV.")
    producer = setup_producer()

    logger.info(f"Start sending messages to topic: {TOPIC_NAME}")

    for index, row in df.iterrows():
        record = row.to_dict()

        logger.info(f"Sending message #{index}: {record}")
        send_with_log(producer, record, index)

        time.sleep(delay)

    logger.info("Flushing producer before shutdown...")
    producer.flush()
    logger.info("All messages flushed and sent successfully!")

if __name__ == "__main__":
    csv_file_path = "example.csv"
    csv_to_kafka(csv_file_path, delay=1.0)
