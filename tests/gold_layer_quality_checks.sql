/*
===============================================================================
Quality Checks – Gold Layer
===============================================================================
Script Purpose:
    This script performs data model quality checks to validate the integrity,
    consistency, and analytical correctness of the Gold Layer.

    The checks focus on:
    - Surrogate key uniqueness in dimension views
    - Referential integrity between fact and dimension views
    - Detection of orphan records in fact views

Usage Notes:
    - These checks do not modify data.
    - Any returned rows indicate a potential data quality issue
      that should be investigated.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customer'
-- ====================================================================
-- Check for uniqueness of customer_key
-- Expectation: No results
SELECT
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customer
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.dim_seller'
-- ====================================================================
-- Check for uniqueness of seller_key
-- Expectation: No results
SELECT
    seller_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_seller
GROUP BY seller_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.dim_product'
-- ====================================================================
-- Check for uniqueness of product_key
-- Expectation: No results
SELECT
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_product
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.dim_date'
-- ====================================================================
-- Check for uniqueness of date_key
-- Expectation: No results
SELECT
    date_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_date
GROUP BY date_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_orders'
-- ====================================================================
-- Validate connectivity between fact_orders and dimensions
-- Expectation: Rows returned indicate orphan relationships

SELECT
    o.order_id,
    o.customer_key,
    o.purchase_date_key
FROM gold.fact_orders o
LEFT JOIN gold.dim_customer c
    ON c.customer_key = o.customer_key
LEFT JOIN gold.dim_date d
    ON d.date_key = o.purchase_date_key
WHERE c.customer_key IS NULL
   OR d.date_key IS NULL;

-- ====================================================================
-- Checking 'gold.fact_order_items'
-- ====================================================================
-- Validate connectivity between fact_order_items and dimensions
-- Expectation: No results
SELECT
    oi.order_id,
    oi.product_key,
    oi.seller_key
FROM gold.fact_order_items oi
LEFT JOIN gold.dim_product p
    ON p.product_key = oi.product_key
LEFT JOIN gold.dim_seller s
    ON s.seller_key = oi.seller_key
WHERE p.product_key IS NULL
   OR s.seller_key IS NULL;

-- ====================================================================
-- Checking 'gold.fact_payments'
-- ====================================================================
-- Validate connectivity between fact_payments and dimensions
-- Expectation: Rows may appear for orders without valid customers
SELECT
    fp.order_id,
    fp.customer_key,
    fp.payment_date_key
FROM gold.fact_payments fp
LEFT JOIN gold.dim_customer c
    ON c.customer_key = fp.customer_key
LEFT JOIN gold.dim_date d
    ON d.date_key = fp.payment_date_key
WHERE c.customer_key IS NULL
   OR d.date_key IS NULL;

-- ====================================================================
-- Checking 'gold.fact_reviews'
-- ====================================================================
-- Validate connectivity between fact_reviews and dimensions
-- Expectation: Rows may appear due to missing customers or dates
SELECT
    fr.review_id,
    fr.order_id,
    fr.customer_key,
    fr.review_creation_date_key
FROM gold.fact_reviews fr
LEFT JOIN gold.dim_customer c
    ON c.customer_key = fr.customer_key
LEFT JOIN gold.dim_date d
    ON d.date_key = fr.review_creation_date_key
WHERE c.customer_key IS NULL
   OR d.date_key IS NULL;

