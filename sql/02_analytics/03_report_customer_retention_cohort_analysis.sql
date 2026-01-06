/* ============================================================
   REPORT 03: CUSTOMER RETENTION COHORT ANALYSIS
   ============================================================

   PURPOSE
   -------
   Measure customer retention by cohort using delivered orders.
   Customers are grouped by first purchase month, and retention
   is tracked across subsequent months.

   ============================================================ */

WITH delivered_orders AS (
    -- Delivered orders with customer and order month
    SELECT
        dc.customer_unique_id,
        DATEFROMPARTS(dp.year, dp.month_number, 1) AS order_month
    FROM gold.fact_orders_tbl fo
    JOIN gold.dim_customer dc
        ON fo.customer_key = dc.customer_key
    JOIN gold.dim_date dp
        ON fo.purchase_date_key = dp.date_key
    WHERE fo.order_status = 'delivered'
),

customer_first_purchase AS (
    -- First purchase month per customer (cohort month)
    SELECT
        customer_unique_id,
        MIN(order_month) AS cohort_month
    FROM delivered_orders
    GROUP BY customer_unique_id
),

customer_activity AS (
    -- One row per customer per active month
    SELECT DISTINCT
        d.customer_unique_id,
        cfp.cohort_month,
        d.order_month,
        DATEDIFF(
            MONTH,
            cfp.cohort_month,
            d.order_month
        ) AS months_since_acquisition
    FROM delivered_orders d
    JOIN customer_first_purchase cfp
        ON d.customer_unique_id = cfp.customer_unique_id
),

cohort_sizes AS (
    -- Size of each cohort (month 0)
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_unique_id) AS cohort_size
    FROM customer_first_purchase
    GROUP BY cohort_month
)

SELECT
    ca.cohort_month,
    ca.months_since_acquisition,

    COUNT(DISTINCT ca.customer_unique_id) AS retained_customers,
    cs.cohort_size,

    COUNT(DISTINCT ca.customer_unique_id) * 100.0
    / cs.cohort_size AS retention_rate_pct

FROM customer_activity ca
JOIN cohort_sizes cs
    ON ca.cohort_month = cs.cohort_month

GROUP BY
    ca.cohort_month,
    ca.months_since_acquisition,
    cs.cohort_size

ORDER BY
    ca.cohort_month,
    ca.months_since_acquisition;
