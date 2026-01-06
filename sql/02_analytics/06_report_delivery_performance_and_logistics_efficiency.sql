/* ============================================================
   REPORT 6: DELIVERY PERFORMANCE & LOGISTICS EFFICIENCY
   ============================================================

   PURPOSE
   -------
   Analyze delivery performance to evaluate:
   - On-time vs late delivery rates
   - Average delivery duration
   - Delivery reliability trends over time
   - Operational efficiency of the logistics process

   This report supports operations optimization,
   SLA monitoring, and customer experience analysis.

   SCOPE & ASSUMPTIONS
   -------------------
   • Uses GOLD layer star schema
   • Uses MATERIALIZED fact table:
       - gold.fact_orders_tbl
   • Delivered orders only
   • On-time delivery = delivered_date <= estimated_date
   • Grain: Month

   ============================================================ */

WITH delivered_orders AS (
    SELECT
        fo.order_id,
        fo.delivery_days,
        fo.delivered_customer_date_key,
        fo.estimated_delivery_date_key
    FROM gold.fact_orders_tbl fo
    WHERE fo.order_status = 'delivered'
),

delivery_enriched AS (
    SELECT
        d.order_id,
        d.delivery_days,

        dd.full_date AS delivered_date,
        de.full_date AS estimated_date,

        CASE
            WHEN dd.full_date <= de.full_date THEN 1
            ELSE 0
        END AS is_on_time
    FROM delivered_orders d
    JOIN gold.dim_date dd
        ON d.delivered_customer_date_key = dd.date_key
    JOIN gold.dim_date de
        ON d.estimated_delivery_date_key = de.date_key
),

monthly_delivery_metrics AS (
    SELECT
        dd.year,
        dd.month_number,
        dd.month_name_full,

        COUNT(order_id)                         AS delivered_orders,
        AVG(delivery_days * 1.0)                AS avg_delivery_days,

        SUM(is_on_time)                         AS on_time_orders,
        COUNT(order_id) - SUM(is_on_time)       AS late_orders
    FROM delivery_enriched de
    JOIN gold.dim_date dd
        ON de.delivered_date = dd.full_date
    GROUP BY
        dd.year,
        dd.month_number,
        dd.month_name_full
)

SELECT
    year,
    month_number,
    month_name_full,

    delivered_orders,
    avg_delivery_days,

    on_time_orders,
    late_orders,

    /* On-Time Delivery Rate (%) */
    on_time_orders * 100.0 / NULLIF(delivered_orders, 0)
        AS on_time_delivery_rate_pct

FROM monthly_delivery_metrics
ORDER BY
    year,
    month_number;
