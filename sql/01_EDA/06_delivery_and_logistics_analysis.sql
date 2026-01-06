/* =================================================================
   EDA – Section 5: Delivery & Logistics Analysis
   Data Source : Olist E-Commerce (Gold Layer)
   Purpose     :
     This section evaluates delivery performance and logistics 
     efficiency. It focuses on delivery time, late deliveries, 
     seller performance, and the relationship between delivery 
     speed, revenue, and customer experience.
===================================================================== */

-- Delivery time distribution (days)
SELECT
    MIN(delivery_days) AS min_delivery_days,
    MAX(delivery_days) AS max_delivery_days,
    AVG(delivery_days) AS avg_delivery_days
FROM gold.fact_orders
WHERE delivery_days IS NOT NULL;

-- Late vs on-time deliveries
SELECT
    CASE
        WHEN delivered_customer_date_key > estimated_delivery_date_key THEN 'Late'
        ELSE 'On Time'
    END AS delivery_status,
    COUNT(*) AS total_orders,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
        2
    ) AS percentage
FROM gold.fact_orders
WHERE delivered_customer_date_key IS NOT NULL
GROUP BY
    CASE
        WHEN delivered_customer_date_key > estimated_delivery_date_key THEN 'Late'
        ELSE 'On Time'
    END;

-- Delivery time vs order value
SELECT
    o.delivery_days,
    COUNT(DISTINCT o.order_id) AS total_orders,
    AVG(oiv.order_value) AS avg_order_value
FROM gold.fact_orders o
JOIN (
    SELECT
        order_id,
        SUM(price + freight_value) AS order_value
    FROM gold.fact_order_items
    GROUP BY order_id
) oiv
    ON o.order_id = oiv.order_id
WHERE o.delivery_days IS NOT NULL
GROUP BY o.delivery_days
ORDER BY o.delivery_days;

-- Average delivery time per seller
SELECT
    ds.seller_id,
    COUNT(DISTINCT o.order_id) AS total_orders,
    AVG(o.delivery_days) AS avg_delivery_days
FROM gold.fact_orders o
JOIN gold.fact_order_items oi
    ON o.order_id = oi.order_id
JOIN gold.dim_seller ds
    ON oi.seller_key = ds.seller_key
WHERE o.delivery_days IS NOT NULL
GROUP BY ds.seller_id
ORDER BY avg_delivery_days;

-- Late delivery rate by seller
WITH OrderSeller AS (
    SELECT DISTINCT
        o.order_id,
        oi.seller_key,
        CASE
            WHEN o.delivered_customer_date_key > o.estimated_delivery_date_key THEN 1
            ELSE 0
        END AS is_late
    FROM gold.fact_orders o
    JOIN gold.fact_order_items oi
        ON o.order_id = oi.order_id
    WHERE o.delivered_customer_date_key IS NOT NULL
)
SELECT
    ds.seller_id,
    COUNT(*) AS total_orders,
    SUM(is_late) AS late_orders,
    ROUND(100.0 * SUM(is_late) / COUNT(*), 2) AS late_delivery_percentage
FROM OrderSeller os
JOIN gold.dim_seller ds
    ON os.seller_key = ds.seller_key
GROUP BY ds.seller_id
ORDER BY late_delivery_percentage DESC;

-- Freight cost vs delivery time
SELECT
    o.delivery_days,
    AVG(oi.freight_value) AS avg_freight_cost,
    COUNT(*) AS total_items
FROM gold.fact_orders o
JOIN gold.fact_order_items oi
    ON o.order_id = oi.order_id
WHERE o.delivery_days IS NOT NULL
GROUP BY o.delivery_days
ORDER BY o.delivery_days;
