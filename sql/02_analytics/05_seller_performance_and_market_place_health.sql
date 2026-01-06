/* ============================================================
   REPORT 5: SELLER PERFORMANCE & MARKETPLACE HEALTH
   ============================================================

   PURPOSE
   -------
   Evaluate seller performance across the marketplace to:
   - Identify top-performing sellers by revenue and volume
   - Measure average order value (AOV) per seller
   - Understand seller contribution concentration
   - Support marketplace health and seller management decisions

   This report is critical for assessing supply-side strength,
   seller dependency risk, and revenue concentration.

   SCOPE & ASSUMPTIONS
   -------------------
   • Uses GOLD layer star schema
   • Uses MATERIALIZED fact tables for performance:
       - gold.fact_orders_tbl
       - gold.fact_order_items_tbl
   • Delivered orders only
   • Revenue = price + freight_value
   • Grain: Seller

   ============================================================ */

WITH delivered_orders AS (
    SELECT
        order_id
    FROM gold.fact_orders_tbl
    WHERE order_status = 'delivered'
),

seller_metrics AS (
    SELECT
        ds.seller_id,
        ds.seller_city,
        ds.seller_state,

        COUNT(DISTINCT foi.order_id)           AS total_orders,
        COUNT(foi.order_item_id)               AS total_items_sold,

        SUM(foi.price + foi.freight_value)     AS total_revenue,
        AVG(foi.price)                         AS avg_item_price
    FROM gold.fact_order_items_tbl foi
    JOIN delivered_orders d
        ON foi.order_id = d.order_id
    JOIN gold.dim_seller ds
        ON foi.seller_key = ds.seller_key
    GROUP BY
        ds.seller_id,
        ds.seller_city,
        ds.seller_state
),

seller_with_totals AS (
    SELECT
        *,
        SUM(total_revenue) OVER () AS marketplace_revenue
    FROM seller_metrics
)

SELECT
    seller_id,
    seller_city,
    seller_state,

    total_orders,
    total_items_sold,

    total_revenue,

    /* Average Order Value */
    total_revenue * 1.0 / NULLIF(total_orders, 0)
        AS avg_order_value,

    avg_item_price,

    /* Seller Revenue Share (%) */
    total_revenue * 100.0 / NULLIF(marketplace_revenue, 0)
        AS revenue_share_pct

FROM seller_with_totals
ORDER BY total_revenue DESC;
