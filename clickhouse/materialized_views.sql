-- ===========================================
--      MATERIALIZED VIEWS (Aggregations)
-- ===========================================

-- Hourly event count and revenue by event type
CREATE
MATERIALIZED VIEW mv_hourly_events
    ENGINE = SummingMergeTree()
    PARTITION BY toYYYYMM(event_hour)
    ORDER BY (event_hour, event_type)
AS
SELECT toStartOfHour(event_time) AS event_hour,
       event_type,
       count()                   AS event_count,
       sum(price)                AS total_revenue
FROM ecommerce_events
GROUP BY event_hour, event_type;

-- View count and average price for top viewed products
CREATE
MATERIALIZED VIEW mv_top_viewed_products
    ENGINE = AggregatingMergeTree()
    ORDER BY (product_id, brand)
AS
SELECT product_id,
       brand,
       count()    AS view_count,
       avg(price) AS avg_price
FROM ecommerce_events
WHERE event_type = 'view'
GROUP BY product_id, brand;

-- Category-level stats: event count, unique users, total value
CREATE
MATERIALIZED VIEW mv_category_stats
    ENGINE = AggregatingMergeTree()
    ORDER BY (category_code, event_type)
AS
SELECT category_code,
       event_type,
       count()       AS event_count,
       uniqState(user_id) AS unique_users,
       sum(price)    AS total_value
FROM ecommerce_events
WHERE category_code != ''
GROUP BY category_code, event_type;

-- User session behavior stats: count, unique products, total value, session timings
CREATE
MATERIALIZED VIEW mv_user_session_behavior
    ENGINE = AggregatingMergeTree()
    ORDER BY (user_session, user_id)
AS
SELECT user_session,
       user_id,
       countState()          AS event_count,
       uniqState(product_id) AS unique_products_viewed,
       sumState(price)       AS total_cart_value,
       minState(event_time)  AS session_start,
       maxState(event_time)  AS session_end
FROM ecommerce_events
GROUP BY user_session, user_id;

-- Daily brand performance: views, purchases, revenue
CREATE
MATERIALIZED VIEW mv_brand_performance
    ENGINE = SummingMergeTree()
    PARTITION BY toYYYYMM(event_date)
    ORDER BY (event_date, brand)
AS
SELECT toDate(event_time)                    AS event_date,
       brand,
       countIf(event_type = 'view')          AS views,
       countIf(event_type = 'purchase')      AS purchases,
       sumIf(price, event_type = 'purchase') AS revenue
FROM ecommerce_events
WHERE brand != ''
GROUP BY event_date, brand;

-- Customer LTV: totals, purchases, revenue, unique products
CREATE
MATERIALIZED VIEW mv_customer_ltv
    ENGINE = SummingMergeTree()
    ORDER BY user_id
AS
SELECT user_id,
       count()                               AS total_events,
       countIf(event_type = 'purchase')      AS purchase_count,
       sumIf(price, event_type = 'purchase') AS lifetime_value,
       uniq(product_id)                      AS unique_products_viewed
FROM ecommerce_events
GROUP BY user_id;

-- Price segment analytics: buckets, event types, avg price, unique buyers
CREATE
MATERIALIZED VIEW mv_price_segments
    ENGINE = AggregatingMergeTree()
    ORDER BY (price_segment, event_type)
AS
SELECT multiIf(
               price < 50, 'budget',
               price < 200, 'mid-range',
               price < 500, 'premium',
               'luxury'
       )             AS price_segment,
       event_type,
       count()       AS event_count,
       avg(price)    AS avg_price,
       uniq(user_id) AS unique_customers
FROM ecommerce_events
GROUP BY price_segment, event_type;

-- Product velocity: daily views, carts, purchases, and avg sales per product
CREATE
MATERIALIZED VIEW mv_product_velocity
    ENGINE = AggregatingMergeTree()
    PARTITION BY toYYYYMM(event_date)
    ORDER BY (event_date, product_id)
AS
SELECT toDate(event_time)                    AS event_date,
       product_id,
       brand,
       category_code,
       countIf(event_type = 'view')          AS daily_views,
       countIf(event_type = 'cart')          AS daily_carts,
       countIf(event_type = 'purchase')      AS daily_purchases,
       avgIf(price, event_type = 'purchase') AS avg_sale_price
FROM ecommerce_events
GROUP BY event_date, product_id, brand, category_code;

-- Product purchase journey for each user: first view, purchase time, and touchpoints
CREATE
MATERIALIZED VIEW mv_purchase_journey
    ENGINE = AggregatingMergeTree()
    ORDER BY (product_id, user_id)
AS
SELECT product_id,
       user_id,
       minState(event_time)                            AS first_view_time,
       maxStateIf(event_time, event_type = 'purchase') AS purchase_time,
       countState()                                    AS touchpoints
FROM ecommerce_events
GROUP BY product_id, user_id;

-- Realtime dashboard metrics every 5 minutes
CREATE
MATERIALIZED VIEW mv_realtime_dashboard
    ENGINE = ReplacingMergeTree()
    ORDER BY metric_time
AS
SELECT toStartOfFiveMinute(event_time)       AS metric_time,
       count()                               AS total_events,
       uniq(user_id)                         AS active_users,
       uniq(user_session)                    AS active_sessions,
       countIf(event_type = 'view')          AS views,
       countIf(event_type = 'cart')          AS add_to_carts,
       countIf(event_type = 'purchase')      AS purchases,
       sumIf(price, event_type = 'purchase') AS revenue_5min,
       avgIf(price, event_type = 'purchase') AS avg_order_value
FROM ecommerce_events
GROUP BY metric_time;