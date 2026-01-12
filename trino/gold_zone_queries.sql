SHOW CATALOGS;
SHOW SCHEMAS FROM hive;

CREATE TABLE hive.default.fact_events (
                                          event_time TIMESTAMP,
                                          event_type VARCHAR,
                                          product_id BIGINT,
                                          category_id BIGINT,
                                          category_code VARCHAR,
                                          brand VARCHAR,
                                          price DECIMAL(10,2),
                                          user_id BIGINT,
                                          user_session VARCHAR,
                                          event_date DATE
)
    WITH (
        format = 'PARQUET',
        external_location = 's3a://ecommerce/ecommerce/events/gold_zone/fact_events',
        partitioned_by = ARRAY['event_date']
        );

CALL hive.system.sync_partition_metadata(
    schema_name => 'default',
    table_name  => 'fact_events',
    mode        => 'ADD'
);


CREATE TABLE hive.default.agg_daily_events (
    total_events BIGINT,
    total_sessions BIGINT,
    total_users BIGINT,
    total_revenue DECIMAL(20,2),
    view_count BIGINT,
    cart_count BIGINT,
    purchase_count BIGINT,
    event_date DATE
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://ecommerce/ecommerce/events/gold_zone/agg_daily_events',
    partitioned_by = ARRAY['event_date']
);

CALL hive.system.sync_partition_metadata(
    schema_name => 'default',
    table_name  => 'agg_daily_events',
    mode        => 'ADD'
);

CREATE TABLE hive.default.agg_product_daily (
    product_id BIGINT,
    category_code VARCHAR,
    brand VARCHAR,
    views BIGINT,
    cart_adds BIGINT,
    purchases BIGINT,
    revenue DECIMAL(20,2),
    event_date DATE
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://ecommerce/ecommerce/events/gold_zone/agg_product_daily',
    partitioned_by = ARRAY['event_date']
);

CALL hive.system.sync_partition_metadata(
    schema_name => 'default',
    table_name  => 'agg_product_daily',
    mode        => 'ADD'
);

-- ============================================
-- QUERIES CHO FACT_EVENTS TABLE
-- ============================================

SELECT * FROM hive.ecommerce.fact_events LIMIT 100;

-- 6. Đọc fact_events theo ngày cụ thể
SELECT
    event_date,
    event_time,
    event_type,
    product_id,
    category_id,
    category_code,
    brand,
    price,
    user_id,
    user_session
FROM hive.default.fact_events
WHERE event_date = DATE '2019-10-01'
ORDER BY event_time DESC;

-- 7. Thống kê số lượng events theo loại trong một ngày
SELECT
    event_date,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT product_id) as unique_products
FROM hive.default.fact_events
WHERE event_date >= DATE '2019-10-01' - INTERVAL '7' DAY
GROUP BY event_date, event_type
ORDER BY event_date DESC, event_type;

-- ============================================
-- QUERIES CHO AGG_DAILY_EVENTS TABLE
-- ============================================

-- 8. Đọc dữ liệu tổng hợp hàng ngày
SELECT
    event_date,
    total_events,
    total_sessions,
    total_users,
    total_revenue,
    view_count,
    cart_count,
    purchase_count
FROM hive.default.agg_daily_events
WHERE event_date >= DATE '2019-10-01' - INTERVAL '30' DAY
ORDER BY event_date DESC;

-- 9. Tính tổng doanh thu và số lượng events trong khoảng thời gian
SELECT
    SUM(total_revenue) as total_revenue_period,
    SUM(total_events) as total_events_period,
    SUM(total_users) as total_unique_users,
    AVG(total_revenue) as avg_daily_revenue
FROM hive.default.agg_daily_events
WHERE event_date BETWEEN DATE '2019-10-01' AND DATE '2019-10-31';

-- ============================================
-- QUERIES CHO AGG_PRODUCT_DAILY TABLE
-- ============================================

-- 10. Đọc top sản phẩm bán chạy nhất trong một ngày
SELECT
    event_date,
    product_id,
    category_code,
    brand,
    views,
    cart_adds,
    purchases,
    revenue
FROM hive.default.agg_product_daily
WHERE event_date = DATE '2019-10-01'
ORDER BY purchases DESC, revenue DESC
LIMIT 50;

-- 11. Tìm sản phẩm có conversion rate cao nhất
SELECT
    product_id,
    category_code,
    brand,
    SUM(views) as total_views,
    SUM(cart_adds) as total_cart_adds,
    SUM(purchases) as total_purchases,
    SUM(revenue) as total_revenue,
    CAST(SUM(purchases) * 100.0 / NULLIF(SUM(views), 0) AS DECIMAL(5,2)) as conversion_rate_pct,
    CAST(SUM(cart_adds) * 100.0 / NULLIF(SUM(views), 0) AS DECIMAL(5,2)) as cart_rate_pct
FROM hive.default.agg_product_daily
WHERE event_date >= DATE '2019-10-01' - INTERVAL '30' DAY
GROUP BY product_id, category_code, brand
HAVING SUM(views) >= 100
ORDER BY conversion_rate_pct DESC
LIMIT 20;

-- 12. Phân tích theo category và brand
SELECT
    category_code,
    brand,
    COUNT(DISTINCT product_id) as product_count,
    SUM(views) as total_views,
    SUM(purchases) as total_purchases,
    SUM(revenue) as total_revenue,
    CAST(SUM(revenue) / NULLIF(SUM(purchases), 0) AS DECIMAL(10,2)) as avg_order_value
FROM hive.default.agg_product_daily
WHERE event_date >= DATE '2019-10-01' - INTERVAL '30' DAY
    AND category_code IS NOT NULL
    AND brand IS NOT NULL
GROUP BY category_code, brand
ORDER BY total_revenue DESC
LIMIT 30;

-- ============================================
-- QUERIES TỔNG HỢP (JOIN MULTIPLE TABLES)
-- ============================================

-- 13. So sánh dữ liệu chi tiết với dữ liệu tổng hợp
SELECT
    agg.event_date,
    agg.total_events,
    agg.total_revenue,
    COUNT(DISTINCT fact.product_id) as unique_products_in_fact,
    COUNT(DISTINCT fact.user_id) as unique_users_in_fact
FROM hive.default.agg_daily_events agg
LEFT JOIN hive.default.fact_events fact
    ON agg.event_date = fact.event_date
WHERE agg.event_date >= DATE '2019-10-01' - INTERVAL '7' DAY
GROUP BY agg.event_date, agg.total_events, agg.total_revenue
ORDER BY agg.event_date DESC;

