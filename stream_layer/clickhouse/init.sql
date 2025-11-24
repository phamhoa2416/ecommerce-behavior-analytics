CREATE TABLE IF NOT EXISTS ecommerce.user_behavior_realtime (
    user_id String,
    event_type String,
    product_id String,
    category_code String,
    brand String,
    price Float32,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY timestamp;
