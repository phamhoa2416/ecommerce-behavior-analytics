-- =============================
-- Main E-commerce Events Table
-- =============================
CREATE TABLE ecommerce_events
(
    event_time    DateTime,
    event_type    String,
    product_id    UInt64,
    category_id   UInt64,
    category_code String,
    brand         String,
    price         Float64,
    user_id       UInt64,
    user_session  String
) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(event_time)
        ORDER BY (event_time, user_id);

