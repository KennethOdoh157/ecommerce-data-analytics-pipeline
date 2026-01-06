/* ==============================================================
   EDA – Section 3: Sales and Revenue Analysis
   Data Source : Olist E-Commerce (Gold Layer)
   Purpose     :
     This section analyzes sales performance and revenue 
     metrics at order, customer, product, and time dimensions. 
     Insights include total revenue, order value distribution, 
     monthly/seasonal trends, top products, and payment behavior.
================================================================= */
-- Total revenue, min/max/avg per order
SELECT
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.price + oi.freight_value) AS total_revenue,
    MIN(oi.price + oi.freight_value) AS min_order_value,
    MAX(oi.price + oi.freight_value) AS max_order_value,
    AVG(oi.price + oi.freight_value) AS avg_order_value
FROM gold.fact_order_items oi
JOIN gold.fact_orders o
    ON oi.order_id = o.order_id;

-- Revenue per customer (to analyze repeat vs one-time)
SELECT
    dc.customer_unique_id,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.price + oi.freight_value) AS revenue
FROM gold.fact_orders o
JOIN gold.fact_order_items oi
    ON o.order_id = oi.order_id
JOIN gold.dim_customer dc
    ON o.customer_key = dc.customer_key
GROUP BY dc.customer_unique_id
ORDER BY revenue DESC;

-- Monthly revenue and order count trend
SELECT
    dp.year,
    dp.month_number,
    SUM(oi.price + oi.freight_value) AS monthly_revenue,
    COUNT(DISTINCT o.order_id) AS monthly_orders
FROM gold.fact_orders o
JOIN gold.fact_order_items oi
    ON o.order_id = oi.order_id
JOIN gold.dim_date dp
    ON o.purchase_date_key = dp.date_key
GROUP BY dp.year, dp.month_number
ORDER BY dp.year, dp.month_number;
