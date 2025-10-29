CREATE TABLE ecommerce_data (
    order_id String,
    user_id String,
    product_id String,
    quantity Int32,
    price Float64,
    event_time DateTime,
    kafka_timestamp DateTime
)
ENGINE = MergeTree()
ORDER BY event_time;
