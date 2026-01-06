/* ============================================================
   REPORT 1: SALES PERFORMANCE – TIME SERIES
   ============================================================

   PURPOSE
   -------
   Provide monthly sales KPIs including total orders, total revenue,
   average order value, and month-over-month growth.

   DATA SOURCES
   ------------
   • gold.fact_orders_tbl
   • gold.fact_order_items_tbl
   • gold.dim_date

   ============================================================ */

WITH order_revenue AS (
    -- Revenue aggregated at order level
    SELECT
        foi.order_id,
        SUM(foi.price + foi.freight_value) AS order_revenue
    FROM gold.fact_order_items_tbl foi
    GROUP BY foi.order_id
),

delivered_orders AS (
    -- Delivered orders joined with revenue and purchase date
    SELECT
        fo.order_id,
        fo.customer_key,
        dr.order_revenue,
        dp.year,
        dp.month_number,
        dp.month_name_full,
        dp.full_date AS purchase_date
    FROM gold.fact_orders_tbl fo
    JOIN order_revenue dr
        ON fo.order_id = dr.order_id
    JOIN gold.dim_date dp
        ON fo.purchase_date_key = dp.date_key
    WHERE fo.order_status = 'delivered'
),

monthly_agg AS (
    -- Aggregate monthly KPIs
    SELECT
        year,
        month_number,
        month_name_full,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(order_revenue) AS total_revenue,
        SUM(order_revenue) * 1.0 / COUNT(DISTINCT order_id) AS avg_order_value
    FROM delivered_orders
    GROUP BY year, month_number, month_name_full
),

monthly_mom AS (
    -- Calculate month-over-month growth
    SELECT
        ma.*,
        LAG(total_orders) OVER (ORDER BY year, month_number) AS prev_month_orders,
        LAG(total_revenue) OVER (ORDER BY year, month_number) AS prev_month_revenue
    FROM monthly_agg ma
)

SELECT
    year,
    month_number,
    month_name_full,
    total_orders,
    total_revenue,
    avg_order_value,
    
    /* MoM Growth (%) */
    CASE 
        WHEN prev_month_orders IS NULL THEN NULL
        WHEN prev_month_orders < 100 THEN NULL
        ELSE (total_orders - prev_month_orders) * 100.0 / prev_month_orders
    END AS orders_mom_pct,

    CASE
        WHEN prev_month_revenue IS NULL THEN NULL
        WHEN prev_month_revenue < 10000 THEN NULL
        ELSE (total_revenue - prev_month_revenue) * 100.0 / prev_month_revenue
    END AS revenue_mom_pct
FROM monthly_mom
ORDER BY year, month_number;
