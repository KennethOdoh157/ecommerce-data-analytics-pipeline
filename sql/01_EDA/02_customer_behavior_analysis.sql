/* =========================================================
   EDA – Section 2: Customer Behavior Analysis
   Data Source : Olist E-Commerce (Gold Layer)
   Purpose     :
     - Analyze customer purchase frequency
     - Identify one-time vs repeat buyers
     - Support retention insights
========================================================= */

-- Total unique customers
SELECT
    COUNT(DISTINCT customer_unique_id) AS total_unique_customers
FROM gold.dim_customer;

-- Orders per customer (using customer_unique_id)
SELECT
    dc.customer_unique_id,
    COUNT(DISTINCT fo.order_id) AS orders_per_customer
FROM gold.fact_orders fo
JOIN gold.dim_customer dc
    ON fo.customer_key = dc.customer_key
GROUP BY dc.customer_unique_id;

-- One-time vs repeat customer classification
WITH customer_orders AS (
    SELECT
        dc.customer_unique_id,
        COUNT(DISTINCT fo.order_id) AS order_count
    FROM gold.fact_orders fo
    JOIN gold.dim_customer dc
        ON fo.customer_key = dc.customer_key
    GROUP BY dc.customer_unique_id
)
SELECT
    CASE 
        WHEN order_count = 1 THEN 'One-time'
        ELSE 'Repeat'
    END AS customer_type,
    COUNT(*) AS customer_count,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
        2
    ) AS percentage
FROM customer_orders
GROUP BY
    CASE 
        WHEN order_count = 1 THEN 'One-time'
        ELSE 'Repeat'
    END;

-- Distribution of orders per customer
SELECT
    orders_per_customer,
    COUNT(*) AS customer_count
FROM (
    SELECT
        dc.customer_unique_id,
        COUNT(DISTINCT fo.order_id) AS orders_per_customer
    FROM gold.fact_orders fo
    JOIN gold.dim_customer dc
        ON fo.customer_key = dc.customer_key
    GROUP BY dc.customer_unique_id
) t
GROUP BY orders_per_customer
ORDER BY orders_per_customer;

-- Top customers by order count
SELECT TOP 10
    dc.customer_unique_id,
    COUNT(DISTINCT fo.order_id) AS total_orders
FROM gold.fact_orders fo
JOIN gold.dim_customer dc
    ON fo.customer_key = dc.customer_key
GROUP BY dc.customer_unique_id
ORDER BY total_orders DESC;

-- Summary statistics
SELECT
    MIN(order_count) AS min_orders,
    MAX(order_count) AS max_orders,
    AVG(order_count) AS avg_orders
FROM (
    SELECT
        dc.customer_unique_id,
        COUNT(DISTINCT fo.order_id) AS order_count
    FROM gold.fact_orders fo
    JOIN gold.dim_customer dc
        ON fo.customer_key = dc.customer_key
    GROUP BY dc.customer_unique_id
) t;

-- Orders without customer mapping
SELECT
    COUNT(*) AS orders_without_customer
FROM gold.fact_orders
WHERE customer_key IS NULL;
