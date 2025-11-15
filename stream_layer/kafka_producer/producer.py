from kafka import KafkaProducer
import csv, json, time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
topic = "ecommerce_behavior"

with open('./data/user_behavior_sample.csv', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        record = {
            "user_id": row["user_id"],
            "event_type": row["event_type"],
            "product_id": row["product_id"],
            "category_code": row["category_code"],
            "brand": row["brand"],
            "price": float(row["price"]) if row["price"] else 0.0,
            "timestamp": row["event_time"]
        }
        producer.send(topic, value=record)
        print("Sent:", record)
        time.sleep(2)

