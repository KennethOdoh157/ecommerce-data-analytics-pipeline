/* ============================================================
   REPORT 7: PAYMENT BEHAVIOR & INSTALLMENT ANALYSIS
   ============================================================

   PURPOSE
   -------
   Analyze customer payment behavior to understand:
   - Preferred payment methods
   - Installment usage patterns
   - Revenue contribution by payment type
   - Financial risk indicators

   DATA
   ----
   • Materialized facts:
       - gold.fact_orders_tbl
       - gold.fact_order_items_tbl
       - gold.fact_payments
   ============================================================ */

WITH order_revenue AS (
    SELECT
        order_id,
        SUM(price + freight_value) AS order_revenue
    FROM gold.fact_order_items_tbl
    GROUP BY order_id
),

payment_metrics AS (
    SELECT
        fp.payment_type,
        COUNT(DISTINCT fp.order_id) AS total_orders,
        SUM(orv.order_revenue) AS total_revenue,
        AVG(fp.payment_installments * 1.0) AS avg_installments,
        SUM(
            CASE 
                WHEN fp.payment_installments > 1 THEN 1 
                ELSE 0 
            END
        ) AS installment_orders
    FROM gold.fact_payments fp
    JOIN order_revenue orv
        ON fp.order_id = orv.order_id
    GROUP BY fp.payment_type
)

SELECT
    payment_type,
    total_orders,
    total_revenue,
    avg_installments,
    installment_orders,
    installment_orders * 100.0 / NULLIF(total_orders, 0) AS installment_usage_rate_pct
FROM payment_metrics
ORDER BY total_revenue DESC;
