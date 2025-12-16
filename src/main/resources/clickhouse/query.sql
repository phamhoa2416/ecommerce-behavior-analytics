SELECT event_date,
       uniqMerge(daily_unique_users) AS DAU,
       uniqMerge(unique_users_buy)   AS DailyUniqueBuyers,
       sum(total_revenue)            AS DailyRevenue,
       sum(total_carts)              AS DailyCarts,
       round(
               (toFloat64(uniqMerge(unique_users_buy)) /
                uniqMerge(daily_unique_users)) * 100, 2
       )                             AS User_Conversion_Rate_Percent,
       round(
               toFloat64(sum(total_revenue)) / sum(total_carts), 2
       )                             AS Daily_AOV
FROM mv_daily_kpi_unified_target
GROUP BY event_date
HAVING DAU > 0
   AND DailyCarts > 0
ORDER BY event_date DESC;

SELECT category_code,
       sum(total_revenue)   AS LifetimeRevenue,
       sum(total_purchases) AS LifetimeSalesQuantity
FROM mv_product_performance_target
WHERE category_code IS NOT NULL
  AND category_code != ''
GROUP BY category_code
ORDER BY LifetimeRevenue DESC, LifetimeSalesQuantity DESC
    LIMIT 10;

SELECT brand,
       sum(total_revenue)                               AS LifetimeRevenue,
       sum(total_views + total_carts + total_purchases) AS TotalEvents,
       sum(total_purchases)                             AS TotalPurchases
FROM mv_product_performance_target
WHERE brand IS NOT NULL
  AND brand != ''
GROUP BY brand
ORDER BY LifetimeRevenue DESC, TotalEvents DESC
    LIMIT 5;

SELECT metric_time,
       sum(total_events)               AS TotalEvents,
       sum(purchases)                  AS TotalPurchases,
       sum(revenue_15min)              AS TotalRevenue,

       uniqMerge(active_users)         AS Realtime_Active_Users,
       uniqMerge(active_sessions)      AS Realtime_Active_Sessions,
       avgMerge(avg_order_value_state) AS Realtime_AOV
FROM mv_realtime_dashboard_target
WHERE metric_time >= subtractHours(now(), 6)
GROUP BY metric_time
ORDER BY metric_time DESC;

SELECT price_bucket,
       sum(total_revenue)                                     AS BucketRevenue,
       sum(total_purchases)                                   AS BucketPurchases,
       sum(total_events)                                      AS BucketTotalEvents,
       (toFloat64(BucketPurchases) / BucketTotalEvents) * 100 AS bucket_conversion_rate
FROM mv_price_bucket_performance_target
GROUP BY price_bucket
ORDER BY BucketRevenue DESC;

SELECT event_date,
       count()                                                    AS total_sessions,
       avg(session_duration_sec)                                  AS avg_session_duration_sec,
       sum(has_purchase)                                          AS sessions_with_purchase,
       (toFloat64(sessions_with_purchase) / total_sessions) * 100 AS session_conversion_rate_percent
FROM mv_user_session_summary_target
GROUP BY event_date
ORDER BY event_date DESC;