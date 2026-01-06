/* ============================================================
   REPORT 0: EXECUTIVE OVERVIEW – BUSINESS KPI SNAPSHOT
   ============================================================

   PURPOSE
   -------
   Provide an executive-level snapshot of overall business
   performance using the Gold layer star schema.

   This report aggregates company-wide KPIs from fact_orders,
   fact_order_items, and related dimensions to support
   strategic decision-making and dashboard reporting.

   DATA SOURCES
   ------------
   • gold.fact_orders_tbl
   • gold.fact_order_items_tbl
   • gold.dim_customer
   • gold.dim_date

   KEY DESIGN DECISIONS
   -------------------
   • Gold layer quality checks confirm no orphan orders
   • Uses customer_key for fact-to-dimension joins
   • Uses customer_unique_id for customer-level analytics
   • Revenue calculated from fact_order_items (price + freight)
   • Delivered orders only used for revenue and AOV

   OUTPUT
   ------
   One-row, dashboard-ready executive KPI summary with percentages
   suitable for Power BI and executive reporting.

   ============================================================ */

WITH valid_orders AS (
    SELECT
        fo.order_id,
        fo.customer_key,
        dc.customer_unique_id,
        fo.order_status,
        fo.delivered_customer_date_key,
        fo.estimated_delivery_date_key
    FROM gold.fact_orders_tbl fo
    JOIN gold.dim_customer dc
        ON fo.customer_key = dc.customer_key
),

order_revenue AS (
    -- Revenue aggregated at order level
    SELECT
        foi.order_id,
        SUM(foi.price + foi.freight_value) AS order_revenue
    FROM gold.fact_order_items_tbl foi
    GROUP BY foi.order_id
),

delivered_orders AS (
    -- Delivered orders enriched with revenue and delivery dates
    SELECT
        vo.order_id,
        vo.customer_unique_id,
        r.order_revenue,
        d_actual.full_date   AS delivered_date,
        d_est.full_date      AS estimated_date
    FROM valid_orders vo
    JOIN order_revenue r
        ON vo.order_id = r.order_id
    JOIN gold.dim_date d_actual
        ON vo.delivered_customer_date_key = d_actual.date_key
    JOIN gold.dim_date d_est
        ON vo.estimated_delivery_date_key = d_est.date_key
    WHERE vo.order_status = 'delivered'
),

customer_order_counts AS (
    -- Orders per customer for repeat rate calculation
    SELECT
        customer_unique_id,
        COUNT(DISTINCT order_id) AS total_orders
    FROM valid_orders
    GROUP BY customer_unique_id
)

SELECT
    /* Orders & Customers */
    COUNT(DISTINCT vo.order_id)                     AS total_orders,
    COUNT(DISTINCT vo.customer_unique_id)           AS active_customers,

    /* Repeat Customer Rate (%) */
    COUNT(DISTINCT CASE
        WHEN coc.total_orders > 1
        THEN vo.customer_unique_id
    END) * 100.0
    / COUNT(DISTINCT vo.customer_unique_id)          AS repeat_customer_rate_pct,

    /* Cancellation Rate (%) */
    COUNT(DISTINCT CASE
        WHEN vo.order_status = 'canceled'
        THEN vo.order_id
    END) * 100.0
    / COUNT(DISTINCT vo.order_id)                    AS cancellation_rate_pct,

    /* Revenue & AOV (Delivered Orders Only) */
    SUM(do.order_revenue)                            AS total_revenue,
    SUM(do.order_revenue) * 1.0
    / COUNT(DISTINCT do.order_id)                    AS avg_order_value,

    /* On-Time Delivery Rate (%) */
    COUNT(CASE
        WHEN do.delivered_date <= do.estimated_date
        THEN 1
    END) * 100.0
    / COUNT(*)                                      AS on_time_delivery_rate_pct

FROM valid_orders vo
LEFT JOIN delivered_orders do
    ON vo.order_id = do.order_id
LEFT JOIN customer_order_counts coc
    ON vo.customer_unique_id = coc.customer_unique_id;
