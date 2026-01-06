/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

--------------------------------------------------
-- dim_date
--------------------------------------------------
IF OBJECT_ID('gold.dim_date', 'V') IS NOT NULL
    DROP VIEW gold.dim_date;
GO

CREATE VIEW gold.dim_date AS
SELECT
    date_key,
    full_date,

    year,
    year_text,
    quarter_number,
    quarter,
    month_number,
    month_text,
    month_name_full,
    month_name_short,
    week_number_iso,
    week_text,
    day_of_month,
    day_of_year,
    day_name_full,
    day_name_short,

    is_weekend,
    is_weekday,

    is_brazilian_holiday,
    holiday_name,

    is_black_friday,
    is_mothers_day,
    is_valentines_day,
    is_childrens_day,
    is_consumers_day,

    fiscal_year,
    fiscal_quarter
FROM silver.dim_date;
GO

--------------------------------------------------
-- dim_customer
--------------------------------------------------
IF OBJECT_ID('gold.dim_customer', 'V') IS NOT NULL
    DROP VIEW gold.dim_customer;
GO

CREATE VIEW gold.dim_customer AS
SELECT
    ROW_NUMBER() OVER (ORDER BY c.customer_unique_id) AS customer_key,

    c.customer_unique_id,
    c.customer_id,

    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,

    g.geolocation_lat,
    g.geolocation_lng
FROM silver.olist_customers_dataset c
LEFT JOIN silver.olist_geolocation_dataset g
    ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix;
GO

--------------------------------------------------
-- dim_seller
--------------------------------------------------
IF OBJECT_ID('gold.dim_seller', 'V') IS NOT NULL
    DROP VIEW gold.dim_seller;
GO

CREATE VIEW gold.dim_seller AS
SELECT
    ROW_NUMBER() OVER (ORDER BY s.seller_id) AS seller_key,

    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,

    g.geolocation_lat,
    g.geolocation_lng
FROM silver.olist_sellers_dataset s
LEFT JOIN silver.olist_geolocation_dataset g
    ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix;
GO

--------------------------------------------------
-- dim_product
--------------------------------------------------
IF OBJECT_ID('gold.dim_product', 'V') IS NOT NULL
    DROP VIEW gold.dim_product;
GO

CREATE VIEW gold.dim_product AS
SELECT
    ROW_NUMBER() OVER (ORDER BY p.product_id) AS product_key,

    p.product_id,
    p.product_category_name,
    t.product_category_name_english AS product_category_name_en,

    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,

    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM silver.olist_products_dataset p
LEFT JOIN silver.product_category_name_translation t
    ON p.product_category_name = t.product_category_name;
GO

--------------------------------------------------
-- dim_geography
--------------------------------------------------
IF OBJECT_ID('gold.dim_geography', 'V') IS NOT NULL
    DROP VIEW gold.dim_geography;
GO

CREATE VIEW gold.dim_geography AS
SELECT
    ROW_NUMBER() OVER (ORDER BY geolocation_zip_code_prefix) AS geography_key,

    geolocation_zip_code_prefix,
    geolocation_city,
    geolocation_state,
    geolocation_lat,
    geolocation_lng
FROM silver.olist_geolocation_dataset;
GO

--------------------------------------------------
-- fact_orders
--------------------------------------------------
IF OBJECT_ID('gold.fact_orders', 'V') IS NOT NULL
    DROP VIEW gold.fact_orders;
GO

CREATE VIEW gold.fact_orders AS
SELECT
    o.order_id,
    dc.customer_key,

    dp.date_key AS purchase_date_key,
    da.date_key AS approved_date_key,
    dd.date_key AS delivered_customer_date_key,
    de.date_key AS estimated_delivery_date_key,

    o.order_status,

    DATEDIFF(
        DAY,
        o.order_purchase_timestamp,
        o.order_delivered_customer_date
    ) AS delivery_days,

    1 AS order_count
FROM silver.olist_orders_dataset o
LEFT JOIN gold.dim_customer dc
    ON o.customer_id = dc.customer_id
LEFT JOIN gold.dim_date dp
    ON CAST(o.order_purchase_timestamp AS DATE) = dp.full_date
LEFT JOIN gold.dim_date da
    ON CAST(o.order_approved_at AS DATE) = da.full_date
LEFT JOIN gold.dim_date dd
    ON CAST(o.order_delivered_customer_date AS DATE) = dd.full_date
LEFT JOIN gold.dim_date de
    ON CAST(o.order_estimated_delivery_date AS DATE) = de.full_date;
GO

--------------------------------------------------
-- fact_order_items
--------------------------------------------------
IF OBJECT_ID('gold.fact_order_items', 'V') IS NOT NULL
    DROP VIEW gold.fact_order_items;
GO

CREATE VIEW gold.fact_order_items AS
SELECT
    oi.order_id,
    oi.order_item_id,

    dp.product_key,
    ds.seller_key,

    dd.date_key AS shipping_limit_date_key,

    oi.price,
    oi.freight_value,
    1 AS item_count
FROM silver.olist_order_items_dataset oi
LEFT JOIN gold.dim_product dp
    ON oi.product_id = dp.product_id
LEFT JOIN gold.dim_seller ds
    ON oi.seller_id = ds.seller_id
LEFT JOIN gold.dim_date dd
    ON CAST(oi.shipping_limit_date AS DATE) = dd.full_date;
GO

--------------------------------------------------
-- fact_payments
--------------------------------------------------
IF OBJECT_ID('gold.fact_payments', 'V') IS NOT NULL
    DROP VIEW gold.fact_payments;
GO

CREATE VIEW gold.fact_payments AS
SELECT
    p.order_id,
    p.payment_sequential,

    dc.customer_key,
    dd.date_key AS payment_date_key,

    p.payment_type,
    p.payment_installments,
    p.payment_value,
    1 AS payment_count
FROM silver.olist_order_payments_dataset p
LEFT JOIN silver.olist_orders_dataset o
    ON p.order_id = o.order_id
LEFT JOIN gold.dim_customer dc
    ON o.customer_id = dc.customer_id
LEFT JOIN gold.dim_date dd
    ON CAST(o.order_purchase_timestamp AS DATE) = dd.full_date;
GO

--------------------------------------------------
-- fact_reviews
--------------------------------------------------
IF OBJECT_ID('gold.fact_reviews', 'V') IS NOT NULL
    DROP VIEW gold.fact_reviews;
GO

CREATE VIEW gold.fact_reviews AS
SELECT
    r.review_id,
    r.order_id,

    dc.customer_key,
    dd.date_key AS review_creation_date_key,

    r.review_score,
    1 AS review_count
FROM silver.olist_order_reviews_dataset r
LEFT JOIN silver.olist_orders_dataset o
    ON r.order_id = o.order_id
LEFT JOIN gold.dim_customer dc
    ON o.customer_id = dc.customer_id
LEFT JOIN gold.dim_date dd
    ON CAST(r.review_creation_date AS DATE) = dd.full_date;
GO
