/* =================================================================
   EDA – Section 5: Payment & Financial Analysis
   Data Source : Olist E-Commerce (Gold Layer)
   Purpose     :
     This section analyzes how customers pay for orders 
     and how payment behavior impacts revenue. 
     It focuses on payment methods, installment patterns, 
     and order value differences across payment types.
===================================================================== */
-- Distribution of payment methods
SELECT
    p.payment_type,
    COUNT(DISTINCT p.order_id) AS total_orders,
    SUM(p.payment_value) AS total_payment_value,
    ROUND(
        100.0 * COUNT(DISTINCT p.order_id)
        / SUM(COUNT(DISTINCT p.order_id)) OVER (),
        2
    ) AS order_percentage
FROM gold.fact_payments p
GROUP BY p.payment_type
ORDER BY total_orders DESC;

-- Installment behavior analysis
SELECT
    p.payment_installments,
    COUNT(DISTINCT p.order_id) AS total_orders,
    SUM(p.payment_value) AS total_payment_value,
    AVG(p.payment_value) AS avg_payment_value
FROM gold.fact_payments p
GROUP BY p.payment_installments
ORDER BY p.payment_installments;

-- Average order value by payment method
SELECT
    p.payment_type,
    COUNT(DISTINCT p.order_id) AS total_orders,
    AVG(oiv.order_value) AS avg_order_value
FROM gold.fact_payments p
JOIN (
    SELECT
        order_id,
        SUM(price + freight_value) AS order_value
    FROM gold.fact_order_items
    GROUP BY order_id
) oiv
    ON p.order_id = oiv.order_id
GROUP BY p.payment_type
ORDER BY avg_order_value DESC;

-- Compare payment value vs calculated order value
SELECT
    p.order_id,
    SUM(p.payment_value) AS total_payment_value,
    oiv.order_value,
    (SUM(p.payment_value) - oiv.order_value) AS payment_difference
FROM gold.fact_payments p
JOIN (
    SELECT
        order_id,
        SUM(price + freight_value) AS order_value
    FROM gold.fact_order_items
    GROUP BY order_id
) oiv
    ON p.order_id = oiv.order_id
GROUP BY p.order_id, oiv.order_value
HAVING ABS(SUM(p.payment_value) - oiv.order_value) > 0.01
ORDER BY payment_difference DESC;

-- Monthly payment trend
SELECT
    d.year,
    d.month_number,
    p.payment_type,
    SUM(p.payment_value) AS monthly_payment_value,
    COUNT(DISTINCT p.order_id) AS monthly_orders
FROM gold.fact_payments p
JOIN gold.dim_date d
    ON p.payment_date_key = d.date_key
GROUP BY d.year, d.month_number, p.payment_type
ORDER BY d.year, d.month_number, p.payment_type;
