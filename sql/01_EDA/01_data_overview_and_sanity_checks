/* =========================================================
   EDA – Section 1: Dataset Overview & Sanity Checks
   Data Source : Olist E-Commerce (Gold Layer)
   Purpose     :
     - Validate Gold layer completeness
     - Confirm time coverage
     - Check entity counts and integrity
========================================================= */
-- Order time coverage
SELECT
    MIN(d.full_date) AS first_order_date,
    MAX(d.full_date) AS last_order_date
FROM gold.fact_orders fo
JOIN gold.dim_date d
    ON fo.purchase_date_key = d.date_key;

-- Core fact table row counts
SELECT 'fact_orders'       AS table_name, COUNT(*) AS row_count FROM gold.fact_orders
UNION ALL
SELECT 'fact_order_items'  AS table_name, COUNT(*) AS row_count FROM gold.fact_order_items
UNION ALL
SELECT 'fact_payments'     AS table_name, COUNT(*) AS row_count FROM gold.fact_payments
UNION ALL
SELECT 'fact_reviews'      AS table_name, COUNT(*) AS row_count FROM gold.fact_reviews;

-- Dimension table row counts
SELECT 'dim_customer'  AS table_name, COUNT(*) AS row_count FROM gold.dim_customer
UNION ALL
SELECT 'dim_product'   AS table_name, COUNT(*) AS row_count FROM gold.dim_product
UNION ALL
SELECT 'dim_seller'    AS table_name, COUNT(*) AS row_count FROM gold.dim_seller
UNION ALL
SELECT 'dim_date'      AS table_name, COUNT(*) AS row_count FROM gold.dim_date
UNION ALL
SELECT 'dim_geography' AS table_name, COUNT(*) AS row_count FROM gold.dim_geography;

-- Distinct entities across facts
SELECT
    COUNT(DISTINCT customer_key) AS distinct_customers,
    COUNT(DISTINCT order_id)     AS distinct_orders
FROM gold.fact_orders;
SELECT
    COUNT(DISTINCT product_key) AS distinct_products,
    COUNT(DISTINCT seller_key)  AS distinct_sellers
FROM gold.fact_order_items;

-- Order status distribution
SELECT
    order_status,
    COUNT(*) AS order_count,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
        2
    ) AS percentage
FROM gold.fact_orders
GROUP BY order_status
ORDER BY order_count DESC;

-- Delivery days distribution
SELECT
    MIN(delivery_days) AS min_delivery_days,
    MAX(delivery_days) AS max_delivery_days,
    AVG(delivery_days) AS avg_delivery_days
FROM gold.fact_orders
WHERE delivery_days IS NOT NULL;
-- Late deliveries
SELECT
    COUNT(*) AS late_orders
FROM gold.fact_orders
WHERE delivered_customer_date_key > estimated_delivery_date_key;

-- Orders without customers
SELECT COUNT(*) AS orphan_orders
FROM gold.fact_orders fo
LEFT JOIN gold.dim_customer dc
    ON fo.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL;

-- Order items without products
SELECT COUNT(*) AS orphan_order_items
FROM gold.fact_order_items foi
LEFT JOIN gold.dim_product dp
    ON foi.product_key = dp.product_key
WHERE dp.product_key IS NULL;

-- Order items without sellers
SELECT COUNT(*) AS orphan_sellers
FROM gold.fact_order_items foi
LEFT JOIN gold.dim_seller ds
    ON foi.seller_key = ds.seller_key
WHERE ds.seller_key IS NULL;

-- Monetary sanity check
SELECT
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    AVG(price) AS avg_price,
    MIN(freight_value) AS min_freight,
    MAX(freight_value) AS max_freight
FROM gold.fact_order_items;

-- Payments sanity check
SELECT
    MIN(payment_value) AS min_payment,
    MAX(payment_value) AS max_payment,
    AVG(payment_value) AS avg_payment
FROM gold.fact_payments;

--High-Level Business Summary
SELECT
    COUNT(DISTINCT fo.order_id)      AS total_orders,
    COUNT(DISTINCT fo.customer_key)  AS total_customers,
    COUNT(DISTINCT foi.product_key)  AS total_products,
    COUNT(DISTINCT foi.seller_key)   AS total_sellers
FROM gold.fact_orders fo
LEFT JOIN gold.fact_order_items foi
    ON fo.order_id = foi.order_id;

