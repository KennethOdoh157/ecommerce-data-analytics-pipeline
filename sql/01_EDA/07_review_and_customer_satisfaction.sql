/* =================================================================
   EDA – Section 5: Review & Customer Satisfaction
   Data Source : Olist E-Commerce (Gold Layer)
   Purpose     :
     This section analyzes customer reviews to understand 
     satisfaction levels, review behavior, and how factors such as 
     delivery performance and customer type influence review scores.
===================================================================== */
-- Distribution of review scores
SELECT
    review_score,
    COUNT(*) AS total_reviews,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
        2
    ) AS percentage
FROM gold.fact_reviews
GROUP BY review_score
ORDER BY review_score;

-- Monthly average review score
SELECT
    d.year,
    d.month_number,
    AVG(r.review_score) AS avg_review_score,
    COUNT(*) AS total_reviews
FROM gold.fact_reviews r
JOIN gold.dim_date d
    ON r.review_creation_date_key = d.date_key
GROUP BY d.year, d.month_number
ORDER BY d.year, d.month_number;

-- Review score vs delivery days
SELECT
    o.delivery_days,
    AVG(r.review_score) AS avg_review_score,
    COUNT(*) AS total_reviews
FROM gold.fact_reviews r
JOIN gold.fact_orders o
    ON r.order_id = o.order_id
WHERE o.delivery_days IS NOT NULL
GROUP BY o.delivery_days
ORDER BY o.delivery_days;

-- Review score for late vs on-time deliveries
SELECT
    CASE
        WHEN o.delivered_customer_date_key > o.estimated_delivery_date_key THEN 'Late'
        ELSE 'On Time'
    END AS delivery_status,
    AVG(r.review_score) AS avg_review_score,
    COUNT(*) AS total_reviews
FROM gold.fact_reviews r
JOIN gold.fact_orders o
    ON r.order_id = o.order_id
WHERE o.delivered_customer_date_key IS NOT NULL
GROUP BY
    CASE
        WHEN o.delivered_customer_date_key > o.estimated_delivery_date_key THEN 'Late'
        ELSE 'On Time'
    END;

-- Review score by customer type
WITH ReviewedOrders AS (
    SELECT DISTINCT
        r.order_id
    FROM gold.fact_reviews r
),
OrderCustomers AS (
    SELECT
        o.order_id,
        dc.customer_unique_id
    FROM gold.fact_orders o
    JOIN ReviewedOrders ro
        ON o.order_id = ro.order_id
    JOIN gold.dim_customer dc
        ON o.customer_key = dc.customer_key
),
CustomerType AS (
    SELECT
        customer_unique_id,
        COUNT(*) AS total_orders
    FROM OrderCustomers
    GROUP BY customer_unique_id
)
SELECT
    CASE
        WHEN ct.total_orders = 1 THEN 'One-Time'
        ELSE 'Repeat'
    END AS customer_type,
    AVG(r.review_score) AS avg_review_score,
    COUNT(*) AS total_reviews
FROM gold.fact_reviews r
JOIN OrderCustomers oc
    ON r.order_id = oc.order_id
JOIN CustomerType ct
    ON oc.customer_unique_id = ct.customer_unique_id
GROUP BY
    CASE
        WHEN ct.total_orders = 1 THEN 'One-Time'
        ELSE 'Repeat'
    END;

-- Orders with and without reviews
SELECT
    CASE
        WHEN r.order_id IS NULL THEN 'No Review'
        ELSE 'Reviewed'
    END AS review_status,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(
        100.0 * COUNT(DISTINCT o.order_id)
        / SUM(COUNT(DISTINCT o.order_id)) OVER (),
        2
    ) AS percentage
FROM gold.fact_orders o
LEFT JOIN gold.fact_reviews r
    ON o.order_id = r.order_id
GROUP BY
    CASE
        WHEN r.order_id IS NULL THEN 'No Review'
        ELSE 'Reviewed'
    END;
