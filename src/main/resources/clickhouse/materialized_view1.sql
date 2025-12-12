-- MV1. Tổng hợp lưu lượng truy cập, hành vi, doanh thu kpi theo ngày
-- Tạo bảng đích (Target Table)
CREATE TABLE mv_daily_kpi_unified_target
(
    event_date              Date,
    total_revenue           Float64,
    total_purchases         UInt64,
    total_views             UInt64,
    total_carts             UInt64,
    daily_unique_users      AggregateFunction(uniq, UInt64), 
    unique_users_buy        AggregateFunction(uniq, UInt64) 
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY event_date;

-- Tạo Materialized View
CREATE MATERIALIZED VIEW mv_daily_kpi_unified
TO mv_daily_kpi_unified_target
AS SELECT
    toDate(event_time) AS event_date,
    sumIf(price, event_type = 'purchase') AS total_revenue,
    countIf(event_type = 'purchase') AS total_purchases,
    countIf(event_type = 'view') AS total_views,
    countIf(event_type = 'cart') AS total_carts,
    uniqState(user_id) AS daily_unique_users,
    uniqIfState(user_id, event_type = 'purchase') AS unique_users_buy
FROM ecommerce_events
GROUP BY event_date;

-- MV2. Hiệu suất sản phẩm theo danh mục và thương hiệu
-- Tạo bảng đích (Target Table)
CREATE TABLE mv_product_performance_target
(
    event_date      Date,
    category_code   String,
    brand           String,
    product_id      UInt64,
    total_views     UInt64,
    total_carts     UInt64,
    total_purchases UInt64,
    total_revenue   Float64
)
ENGINE = SummingMergeTree(total_revenue, total_views, total_carts, total_purchases)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, category_code, brand, product_id);

-- Tạo Materialized View
CREATE MATERIALIZED VIEW mv_product_performance
TO mv_product_performance_target
AS SELECT
    toDate(event_time) AS event_date,
    category_code,
    brand,
    product_id,
    countIf(event_type = 'view') AS total_views,
    countIf(event_type = 'cart') AS total_carts,
    countIf(event_type = 'purchase') AS total_purchases,
    sumIf(price, event_type = 'purchase') AS total_revenue
FROM ecommerce_events
GROUP BY event_date, category_code, brand, product_id;

-- MV3. Realtime DashBoard trong
-- Tạo bảng đích (Target Table)
CREATE TABLE mv_realtime_dashboard_target
(
    metric_time             DateTime,
    total_events            UInt64,
    views                   UInt64,
    add_to_carts            UInt64,
    purchases               UInt64,
    revenue_15min            Float64,
    active_users            AggregateFunction(uniq, UInt64),
    active_sessions         AggregateFunction(uniq, String),
    avg_order_value_state   AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(metric_time)
ORDER BY metric_time;
-- Tạo Materialized View
CREATE MATERIALIZED VIEW mv_realtime_dashboard
TO mv_realtime_dashboard_target
AS SELECT
    toStartOfFifteenMinute(event_time) AS metric_time,
    count() AS total_events,
    countIf(event_type = 'view') AS views,
    countIf(event_type = 'cart') AS add_to_carts,
    countIf(event_type = 'purchase') AS purchases,
    sumIf(price, event_type = 'purchase') AS revenue_15min,
    uniqState(user_id) AS active_users,
    uniqState(user_session) AS active_sessions,
    avgIfState(price, event_type = 'purchase') AS avg_order_value_state
FROM ecommerce_events
GROUP BY metric_time;

-- MV4. Hiệu suất theo phân khúc giá sản phẩm
-- Tạo bảng đích (Target Table)
CREATE TABLE mv_price_bucket_performance_target
(
    event_date      Date,
    price_bucket    String,
    total_events    UInt64,
    total_revenue   Float64,
    total_purchases UInt64
)
ENGINE = SummingMergeTree(total_events, total_revenue, total_purchases)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, price_bucket);

-- Tạo Materialized View
CREATE MATERIALIZED VIEW mv_price_bucket_performance
TO mv_price_bucket_performance_target
AS SELECT
    toDate(event_time) AS event_date,
    -- Nhóm giá sản phẩm
    CASE
        WHEN price <= 10.0 THEN '0-10$'
        WHEN price > 10.0 AND price <= 50.0 THEN '11-50$'
        WHEN price > 50.0 AND price <= 200.0 THEN '51-200$'
        ELSE '>200$'
    END AS price_bucket,
    count() AS total_events,
    sumIf(price, event_type = 'purchase') AS total_revenue,
    countIf(event_type = 'purchase') AS total_purchases
FROM ecommerce_events
WHERE event_type IN ('view', 'purchase')
GROUP BY event_date, price_bucket;

-- MV5. CHuỗi hành động của người dùng trong các phiên
-- Tạo bảng đích (Target Table)
CREATE TABLE mv_user_session_summary_target
(
    user_session        String,
    user_id             UInt64,
    event_date          Date,
    session_start_time  DateTime,
    session_end_time    DateTime,
    session_duration_sec UInt64,
    has_purchase        UInt8, -- 1 nếu có mua hàng
    total_events        UInt64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (user_id, user_session);

-- Tạo Materialized View
CREATE MATERIALIZED VIEW mv_user_session_summary
TO mv_user_session_summary_target
AS SELECT
    user_session,
    user_id,
    toDate(min(event_time)) AS event_date,
    min(event_time) AS session_start_time,
    max(event_time) AS session_end_time,
    dateDiff('second', min(event_time), max(event_time)) AS session_duration_sec,
    max(if(event_type = 'purchase', 1, 0)) AS has_purchase,
    count() AS total_events
FROM ecommerce_events
GROUP BY user_session, user_id;