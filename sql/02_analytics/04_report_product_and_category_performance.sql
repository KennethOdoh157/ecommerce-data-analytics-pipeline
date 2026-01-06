/* ============================================================
   REPORT 3: PRODUCT & CATEGORY PERFORMANCE
   ============================================================

   PURPOSE
   -------
   Analyze product category performance to understand:
   - Which categories drive the most revenue
   - Volume vs value trade-offs across categories
   - Average Order Value (AOV) by category
   - Revenue concentration and category contribution

   This report supports merchandising strategy,
   pricing decisions, and executive performance reviews.

   SCOPE & ASSUMPTIONS
   -------------------
   • Uses GOLD layer star schema
   • Uses MATERIALIZED fact tables for performance:
       - gold.fact_orders_tbl
       - gold.fact_order_items_tbl
   • Delivered orders only
   • Revenue = price + freight_value
   • Grain: Product Category

   ============================================================ */

WITH delivered_orders AS (
    SELECT
        order_id
    FROM gold.fact_orders_tbl
    WHERE order_status = 'delivered'
),

category_metrics AS (
    SELECT
        dp.product_category_name_en        AS product_category,

        COUNT(DISTINCT foi.order_id)       AS total_orders,
        COUNT(foi.order_item_id)           AS total_items_sold,

        SUM(foi.price + foi.freight_value) AS total_revenue,
        AVG(foi.price)                     AS avg_item_price
    FROM gold.fact_order_items_tbl foi
    JOIN delivered_orders d
        ON foi.order_id = d.order_id
    JOIN gold.dim_product dp
        ON foi.product_key = dp.product_key
    GROUP BY
        dp.product_category_name_en
),

category_with_totals AS (
    SELECT
        *,
        SUM(total_revenue) OVER () AS overall_revenue
    FROM category_metrics
)

SELECT
    product_category,

    total_orders,
    total_items_sold,

    total_revenue,

    /* Average Order Value */
    total_revenue * 1.0 / NULLIF(total_orders, 0)
        AS avg_order_value,

    avg_item_price,

    /* Revenue Share (%) */
    total_revenue * 100.0 / NULLIF(overall_revenue, 0)
        AS revenue_share_pct

FROM category_with_totals
ORDER BY total_revenue DESC;
