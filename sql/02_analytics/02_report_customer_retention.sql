/* ============================================================
   REPORT 2: CUSTOMER BEHAVIOR & RETENTION
   ============================================================

   PURPOSE
   -------
   Analyze customer acquisition, repeat behavior, and retention
   at a CUSTOMER-MONTH grain using delivered orders.

   This report correctly classifies each customer as either
   NEW or REPEAT per month (never both), ensuring:

       new_customers + repeat_customers = total_customers

   ============================================================ */

WITH delivered_orders AS (
    -- Delivered orders with customer and purchase month
    SELECT
        fo.order_id,
        dc.customer_unique_id,
        dp.year,
        dp.month_number,
        dp.month_name_full,
        DATEFROMPARTS(dp.year, dp.month_number, 1) AS order_month
    FROM gold.fact_orders_tbl fo
    JOIN gold.dim_customer dc
        ON fo.customer_key = dc.customer_key
    JOIN gold.dim_date dp
        ON fo.purchase_date_key = dp.date_key
    WHERE fo.order_status = 'delivered'
),

customer_first_purchase AS (
    -- First-ever purchase month per customer
    SELECT
        customer_unique_id,
        MIN(order_month) AS first_purchase_month
    FROM delivered_orders
    GROUP BY customer_unique_id
),

customer_monthly AS (
    -- One row per customer per month
    SELECT DISTINCT
        customer_unique_id,
        year,
        month_number,
        month_name_full,
        order_month
    FROM delivered_orders
),

customer_classification AS (
    -- Classify customer ONCE per month
    SELECT
        cm.customer_unique_id,
        cm.year,
        cm.month_number,
        cm.month_name_full,
        CASE
            WHEN cm.order_month = cfp.first_purchase_month
            THEN 'NEW'
            ELSE 'REPEAT'
        END AS customer_type
    FROM customer_monthly cm
    JOIN customer_first_purchase cfp
        ON cm.customer_unique_id = cfp.customer_unique_id
),

order_revenue AS (
    -- Revenue per order
    SELECT
        foi.order_id,
        SUM(foi.price + foi.freight_value) AS order_revenue
    FROM gold.fact_order_items_tbl foi
    GROUP BY foi.order_id
),

monthly_revenue AS (
    -- Monthly revenue and order count
    SELECT
        d.year,
        d.month_number,
        d.month_name_full,
        COUNT(DISTINCT d.order_id) AS total_orders,
        SUM(orv.order_revenue) AS total_revenue
    FROM delivered_orders d
    JOIN order_revenue orv
        ON d.order_id = orv.order_id
    GROUP BY
        d.year,
        d.month_number,
        d.month_name_full
)

SELECT
    cc.year,
    cc.month_number,
    cc.month_name_full,

    /* Customer Counts */
    COUNT(DISTINCT CASE
        WHEN cc.customer_type = 'NEW'
        THEN cc.customer_unique_id
    END) AS new_customers,

    COUNT(DISTINCT CASE
        WHEN cc.customer_type = 'REPEAT'
        THEN cc.customer_unique_id
    END) AS repeat_customers,

    COUNT(DISTINCT cc.customer_unique_id) AS total_customers,

    /* Repeat Rate (%) */
    COUNT(DISTINCT CASE
        WHEN cc.customer_type = 'REPEAT'
        THEN cc.customer_unique_id
    END) * 100.0
    / NULLIF(COUNT(DISTINCT cc.customer_unique_id), 0)
        AS repeat_customer_rate_pct,

    /* Avg Orders per Customer */
    mr.total_orders * 1.0
    / NULLIF(COUNT(DISTINCT cc.customer_unique_id), 0)
        AS avg_orders_per_customer,

    /* Revenue per Customer */
    mr.total_revenue * 1.0
    / NULLIF(COUNT(DISTINCT cc.customer_unique_id), 0)
        AS revenue_per_customer

FROM customer_classification cc
JOIN monthly_revenue mr
    ON cc.year = mr.year
   AND cc.month_number = mr.month_number
   AND cc.month_name_full = mr.month_name_full

GROUP BY
    cc.year,
    cc.month_number,
    cc.month_name_full,
    mr.total_orders,
    mr.total_revenue

ORDER BY
    cc.year,
    cc.month_number;
