-- From MV1. Tỷ lệ chuyển đổi (số lượng mua hàng trên số người dùng duy nhất)
SELECT
    event_date,
    uniqMerge(daily_unique_users) AS DAU,
    uniqMerge(unique_users_buy) AS DailyUniqueBuyers,
    sum(total_revenue) AS DailyRevenue,
    sum(total_carts) AS DailyCarts,
    -- Tỷ lệ Chuyển đổi: (Unique Buyers / DAU)
    round((toFloat64(DailyUniqueBuyers) / DAU) * 100, 2) AS User_Conversion_Rate_Percent,
    -- Tỷ lệ chuyển đổi Giỏ hàng: (Revenue / Carts)
    round(toFloat64(DailyRevenue) / DailyCarts, 2) AS Daily_AOV
FROM mv_daily_kpi_unified_target
WHERE DAU > 0 AND DailyCarts > 0
GROUP BY event_date
ORDER BY event_date DESC;

-- From MV2. Top 10 danh mục sản phẩm đem lại doanh thu cao nhât & số lượng bán
SELECT
    category_code,
    sum(total_revenue) AS LifetimeRevenue,
    sum(total_purchases) AS LifetimeSalesQuantity
FROM mv_product_performance_target
WHERE category_code IS NOT NULL AND category_code != '' 
GROUP BY category_code
ORDER BY LifetimeRevenue DESC, LifetimeSalesQuantity DESC -- Ưu tiên xếp hạng theo Doanh thu, sau đó là Số lượng bán
LIMIT 10;

-- From MV2. Top 5 brand đạt doanh thu cao nhất & có số lượt event lớn (thu hút số người dùng?)
SELECT
    brand,
    sum(total_revenue) AS LifetimeRevenue,
    sum(total_views + total_carts + total_purchases) AS TotalEvents,
    sum(total_purchases) AS TotalPurchases
FROM mv_product_performance_target
WHERE brand IS NOT NULL AND brand != '' 
GROUP BY brand
ORDER BY LifetimeRevenue DESC, TotalEvents DESC -- Ưu tiên xếp hạng theo Doanh thu, sau đó là Tổng Sự kiện
LIMIT 5;

-- From MV3. Hiển thị dữ liệu tôgnr hợp trong 15p
SELECT
    metric_time,
    sum(total_events) AS TotalEvents,
    sum(purchases) AS TotalPurchases,
    sum(revenue_15min) AS TotalRevenue,
    
    -- Lấy kết quả chính xác từ trạng thái AggregateFunction
    uniqMerge(active_users) AS Realtime_Active_Users,
    uniqMerge(active_sessions) AS Realtime_Active_Sessions,
    avgMerge(avg_order_value_state) AS Realtime_AOV
    
FROM mv_realtime_dashboard_target
-- Hiển thị 24 phần tử gần nhất (24 * 15 phút = 6 giờ) để có cái nhìn tổng quát hơn
WHERE metric_time >= subtractHours(now(), 6) 
GROUP BY metric_time
ORDER BY metric_time DESC;

-- From MV4. Tỷ lệ chuyển đổi và Doanh thu theo phân khúc giá 
SELECT
    price_bucket,
    sum(total_revenue) AS BucketRevenue,
    sum(total_purchases) AS BucketPurchases,
    sum(total_events) AS BucketTotalEvents,
    -- Tính Tỷ lệ chuyển đổi cho từng nhóm giá
    (toFloat64(BucketPurchases) / BucketTotalEvents) * 100 AS bucket_conversion_rate
FROM mv_price_bucket_performance_target
GROUP BY price_bucket
ORDER BY BucketRevenue DESC;

-- From MV5. Tỷ lệ chuyển đổi (số lượt mua hàng trên tổng số session)
SELECT
    event_date,
    count() AS total_sessions,
    avg(session_duration_sec) AS avg_session_duration_sec,
    sum(has_purchase) AS sessions_with_purchase,
    -- Tỷ lệ chuyển đổi phiên
    (toFloat64(sessions_with_purchase) / total_sessions) * 100 AS session_conversion_rate_percent
FROM mv_user_session_summary_target
GROUP BY event_date
ORDER BY event_date DESC;

--