-- Hourly summary: event count, users, sessions, average price
SELECT toHour(event_time) AS hour_of_day,
       count()            AS total_events,
       uniq(user_id)      AS unique_users,
       uniq(user_session) AS unique_sessions,
       avg(price)         AS avg_price
FROM ecommerce_events
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- Daily aggregation of purchase and cart events for transactions and revenue
SELECT toDate(event_time) AS date,
       event_type,
       count()            AS transactions,
       sum(price)         AS revenue,
       avg(price)         AS avg_order_value
FROM ecommerce_events
WHERE event_type IN ('purchase', 'cart')
GROUP BY date, event_type
ORDER BY date DESC;

-- Daily cart abandonment rate calculation
SELECT toDate(event_time)                                    AS date,
       countIf(event_type = 'cart')                          AS carts_created,
       countIf(event_type = 'purchase')                      AS purchases_completed,
       (carts_created - purchases_completed) / carts_created AS abandonment_rate
FROM ecommerce_events
GROUP BY date
ORDER BY date DESC;
