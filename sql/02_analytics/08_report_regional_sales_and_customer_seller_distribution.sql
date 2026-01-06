/* ============================================================
   REPORT 8: REGIONAL SALES & CUSTOMER/SELLER DISTRIBUTION
   ============================================================

   PURPOSE
   -------
   Analyze regional performance across customers and sellers to identify:
   - Top cities and states by number of buyers and sellers
   - Total orders and revenue contribution by region
   - Regional distribution of the business

   DATA
   ----
   • Materialized facts:
       - gold.fact_orders_tbl
       - gold.fact_order_items_tbl
   • Dimensions:
       - gold.dim_customer
       - gold.dim_seller
============================================================ */

WITH customer_orders AS (
    SELECT
        c.customer_city,
        c.customer_state,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COUNT(DISTINCT c.customer_unique_id) AS total_buyers
    FROM gold.fact_orders_tbl o
    JOIN gold.dim_customer c
        ON o.customer_key = c.customer_key
    GROUP BY c.customer_city, c.customer_state
),

seller_orders AS (
    SELECT
        s.seller_city,
        s.seller_state,
        COUNT(DISTINCT oi.order_id) AS total_orders,
        COUNT(DISTINCT s.seller_key) AS total_sellers
    FROM gold.fact_order_items_tbl oi
    JOIN gold.dim_seller s
        ON oi.seller_key = s.seller_key
    GROUP BY s.seller_city, s.seller_state
),

revenue_by_customer_city AS (
    SELECT
        c.customer_city,
        c.customer_state,
        SUM(oi.price + oi.freight_value) AS total_revenue
    FROM gold.fact_order_items_tbl oi
    JOIN gold.fact_orders_tbl o
        ON oi.order_id = o.order_id
    JOIN gold.dim_customer c
        ON o.customer_key = c.customer_key
    GROUP BY c.customer_city, c.customer_state
)

SELECT
    co.customer_city,
    co.customer_state,
    co.total_buyers,
    so.total_sellers,
    co.total_orders AS orders_by_customers,
    so.total_orders AS orders_by_sellers,
    COALESCE(rb.total_revenue, 0) AS total_revenue
FROM customer_orders co
LEFT JOIN seller_orders so
    ON co.customer_city = so.seller_city
    AND co.customer_state = so.seller_state
LEFT JOIN revenue_by_customer_city rb
    ON co.customer_city = rb.customer_city
    AND co.customer_state = rb.customer_state
ORDER BY total_revenue DESC, orders_by_customers DESC;
