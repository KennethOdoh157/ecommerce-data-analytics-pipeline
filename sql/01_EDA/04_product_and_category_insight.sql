/* =================================================================
   EDA – Section 4: Product & Category Insight
   Data Source : Olist E-Commerce (Gold Layer)
   Purpose     :
     This section analyzes products and categories to identify 
     top-selling items, revenue contribution, and product attributes 
     that may influence sales. Insights will help in understanding 
     which products generate the most revenue and how category and 
     product characteristics relate to sales.
===================================================================== */
-- Identify top products by total revenue
SELECT
    dp.product_id,
    dp.product_category_name_en AS category,
    SUM(oi.price) AS total_product_revenue,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    COUNT(*) AS total_items_sold
FROM gold.fact_order_items oi
JOIN gold.dim_product dp
    ON oi.product_key = dp.product_key
JOIN gold.fact_orders o
    ON oi.order_id = o.order_id
GROUP BY dp.product_id, dp.product_category_name_en
ORDER BY total_product_revenue DESC;

-- Aggregate revenue by category
SELECT
    dp.product_category_name_en AS category,
    SUM(oi.price) AS total_revenue,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    COUNT(*) AS total_items_sold
FROM gold.fact_order_items oi
JOIN gold.dim_product dp
    ON oi.product_key = dp.product_key
JOIN gold.fact_orders o
    ON oi.order_id = o.order_id
GROUP BY dp.product_category_name_en
ORDER BY total_revenue DESC;

-- Explore product attributes vs revenue
SELECT
    dp.product_category_name_en AS category,
    AVG(dp.product_weight_g) AS avg_weight_g,
    AVG(dp.product_length_cm) AS avg_length_cm,
    AVG(dp.product_height_cm) AS avg_height_cm,
    AVG(dp.product_width_cm) AS avg_width_cm,
    AVG(dp.product_photos_qty) AS avg_photos,
    SUM(oi.price) AS total_revenue,
    COUNT(DISTINCT oi.order_id) AS total_orders
FROM gold.fact_order_items oi
JOIN gold.dim_product dp
    ON oi.product_key = dp.product_key
JOIN gold.fact_orders o
    ON oi.order_id = o.order_id
GROUP BY dp.product_category_name_en
ORDER BY total_revenue DESC;

-- Distribution of product sales quantities
SELECT
    dp.product_category_name_en AS category,
    COUNT(*) AS items_sold,
    AVG(oi.price) AS avg_price,
    MIN(oi.price) AS min_price,
    MAX(oi.price) AS max_price
FROM gold.fact_order_items oi
JOIN gold.dim_product dp
    ON oi.product_key = dp.product_key
GROUP BY dp.product_category_name_en
ORDER BY items_sold DESC;
