/*
===============================================================================
Script Name:     bronze_layer_quality_checks.sql
===============================================================================
Purpose:
    This script performs comprehensive data quality validations on the 
    Olist e-commerce datasets in the 'bronze' (staging) layer.

    The objective is to assess raw ingested data for structural integrity,
    completeness, and basic business rule compliance before transformation
    into the Silver layer.

    These checks help identify data issues early in the pipeline and prevent
    propagation of errors into downstream analytical layers.

Target Schema:
    bronze

Target Tables (Olist E-commerce Datasets):
    - bronze.olist_customers_dataset
    - bronze.olist_orders_dataset
    - bronze.olist_order_items_dataset
    - bronze.olist_order_payments_dataset
    - bronze.olist_order_reviews_dataset
    - bronze.olist_products_dataset
    - bronze.olist_sellers_dataset
    - bronze.olist_geolocation_dataset
    - bronze.product_category_name_translation

Quality Check Coverage:
    - NULL validation on critical business and technical columns
    - Duplicate detection on primary and business keys
    - Referential integrity checks between related entities
    - Data type, format, and domain validation
    - Logical consistency checks for dates, quantities, and monetary values

Usage Notes:
    - Execute this script immediately after raw data ingestion.
    - Any failing checks indicate data quality issues that must be addressed
      prior to Silver layer cleansing and enrichment.
    - Script is written and optimized for Microsoft SQL Server.

===============================================================================
*/
-- QC 1: Check if customer_id contains NULL values
SELECT 
    COUNT(*) AS null_customer_id_count
FROM bronze.olist_customers_dataset
WHERE customer_id IS NULL;
-- QC 2: Check for empty or whitespace-only customer_id values
SELECT 
    COUNT(*) AS empty_customer_id_count
FROM bronze.olist_customers_dataset
WHERE LTRIM(RTRIM(customer_id)) = '';
-- QC 3: Check for duplicated customer_id values
SELECT 
    customer_id,
    COUNT(*) AS occurrences
FROM bronze.olist_customers_dataset
GROUP BY customer_id
HAVING COUNT(*) > 1;
-- QC 4: Check customer_id values that do not follow expected UUID-like format (32 hex chars)
SELECT 
    customer_id
FROM bronze.olist_customers_dataset
WHERE LEN(customer_id) <> 32
   OR customer_id LIKE '%[^0-9A-Fa-f]%';
-- QC 5: Compare total rows to distinct customer_id count
SELECT
    (SELECT COUNT(*) FROM bronze.olist_customers_dataset) AS total_rows,
    (SELECT COUNT(DISTINCT customer_id) FROM bronze.olist_customers_dataset) AS distinct_customer_ids;

-- QC 1: Check if customer_unique_id contains NULL values
SELECT 
    COUNT(*) AS null_customer_unique_id_count
FROM bronze.olist_customers_dataset
WHERE customer_unique_id IS NULL;
-- QC 2: Check for empty or whitespace-only customer_unique_id values
SELECT 
    COUNT(*) AS empty_customer_unique_id_count
FROM bronze.olist_customers_dataset
WHERE LTRIM(RTRIM(customer_unique_id)) = '';
-- QC 3: Identify customer_unique_id values with invalid length or invalid characters
SELECT
    customer_unique_id
FROM bronze.olist_customers_dataset
WHERE LEN(customer_unique_id) <> 32
   OR customer_unique_id LIKE '%[^0-9A-Fa-f]%';
-- QC 4: Check how many customer_unique_id values appear more than once
SELECT 
    customer_unique_id,
    COUNT(*) AS occurrences
FROM bronze.olist_customers_dataset
GROUP BY customer_unique_id
HAVING COUNT(*) > 1;
-- QC 5: Compare total_rows vs distinct customer_unique_id count
SELECT
    (SELECT COUNT(*) FROM bronze.olist_customers_dataset) AS total_rows,
    (SELECT COUNT(DISTINCT customer_unique_id) FROM bronze.olist_customers_dataset) AS distinct_customer_unique_ids;
-- QC 6: Verify customer_unique_id links to multiple customer_id values (check if unexpected)
SELECT
    customer_unique_id,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids
FROM bronze.olist_customers_dataset
GROUP BY customer_unique_id
HAVING COUNT(DISTINCT customer_id) > 1;
-- QC 1: Check if customer_zip_code_prefix contains NULL values
SELECT 
    COUNT(*) AS null_zip_prefix_count
FROM bronze.olist_customers_dataset
WHERE customer_zip_code_prefix IS NULL;
-- QC 2: Identify non-numeric ZIP prefixes
SELECT
    customer_zip_code_prefix
FROM bronze.olist_customers_dataset
WHERE customer_zip_code_prefix NOT LIKE '%[0-9]%'
   OR TRY_CAST(customer_zip_code_prefix AS INT) IS NULL;
-- QC 3: Identify ZIP prefixes with invalid length
SELECT
    customer_zip_code_prefix,
    LEN(customer_zip_code_prefix) AS prefix_length
FROM bronze.olist_customers_dataset
WHERE LEN(customer_zip_code_prefix) <> 5;
-- QC 4: Detect ZIP prefixes with fewer than 5 digits (possible missing leading zero)
SELECT
    customer_zip_code_prefix
FROM bronze.olist_customers_dataset
WHERE LEN(customer_zip_code_prefix) < 5;
-- QC 5: Identify the top/bottom ZIP prefixes by frequency
SELECT
    customer_zip_code_prefix,
    COUNT(*) AS freq
FROM bronze.olist_customers_dataset
GROUP BY customer_zip_code_prefix
ORDER BY freq ASC; -- Use DESC if you want most common first
-- QC 6: Detect ZIP prefixes that map to multiple customer_state values
SELECT
    customer_zip_code_prefix,
    COUNT(DISTINCT customer_state) AS distinct_states
FROM bronze.olist_customers_dataset
GROUP BY customer_zip_code_prefix
HAVING COUNT(DISTINCT customer_state) > 1;
-- QC 7: Detect ZIP prefixes that map to multiple customer_city values
SELECT
    customer_zip_code_prefix,
    COUNT(DISTINCT customer_city) AS distinct_cities
FROM bronze.olist_customers_dataset
GROUP BY customer_zip_code_prefix
HAVING COUNT(DISTINCT customer_city) > 1;
-- QC 1: Check if customer_city contains NULL values
SELECT 
    COUNT(*) AS null_city_count
FROM bronze.olist_customers_dataset
WHERE customer_city IS NULL;
-- QC 2: Identify blank or whitespace-only values
SELECT 
    COUNT(*) AS blank_city_count
FROM bronze.olist_customers_dataset
WHERE LTRIM(RTRIM(customer_city)) = '';
------------------------------------------------------------
-- QC: customer_city – non-alphabetic characters check
------------------------------------------------------------
SELECT *
FROM bronze.olist_customers_dataset
WHERE customer_city LIKE '%[^a-zA-ZÀ-ÿ ]%';
-- QC 6: Very short city values (likely dirty)
SELECT
    customer_city
FROM bronze.olist_customers_dataset
WHERE LEN(LTRIM(RTRIM(customer_city))) < 3;
-- QC 7: Identify casing inconsistencies
SELECT 
    customer_city,
    CASE 
        WHEN customer_city = UPPER(customer_city) THEN 'UPPERCASE'
        WHEN customer_city = LOWER(customer_city) THEN 'lowercase'
        ELSE 'Mixed Case'
    END AS casing_type
FROM bronze.olist_customers_dataset
GROUP BY customer_city;
-- QC 8: Detect cities mapped to >1 state
SELECT
    customer_city,
    COUNT(DISTINCT customer_state) AS distinct_states
FROM bronze.olist_customers_dataset
GROUP BY customer_city
HAVING COUNT(DISTINCT customer_state) > 1;
-- QC 9: Frequency distribution of cities
SELECT
    customer_city,
    COUNT(*) AS freq
FROM bronze.olist_customers_dataset
GROUP BY customer_city
ORDER BY freq ASC;   -- ASC to see rare/dirty cities first

-- QC 1: Check if customer_state contains NULL values
SELECT 
    COUNT(*) AS null_state_count
FROM bronze.olist_customers_dataset
WHERE customer_state IS NULL;
-- QC 2: Identify blank or whitespace-only values
SELECT 
    COUNT(*) AS blank_state_count
FROM bronze.olist_customers_dataset
WHERE LTRIM(RTRIM(customer_state)) = '';
-- QC 3: Detect invalid length state codes
SELECT 
    customer_state,
    COUNT(*) AS freq
FROM bronze.olist_customers_dataset
WHERE LEN(LTRIM(RTRIM(customer_state))) <> 2
GROUP BY customer_state;
-- QC 4: Identify inconsistent casing
SELECT
    customer_state,
    CASE 
        WHEN customer_state = UPPER(customer_state) THEN 'UPPERCASE'
        WHEN customer_state = LOWER(customer_state) THEN 'lowercase'
        ELSE 'Mixed Case'
    END AS casing_type
FROM bronze.olist_customers_dataset
GROUP BY customer_state;
-- QC 5: Detect states containing non-letter characters
SELECT
    customer_state
FROM bronze.olist_customers_dataset
WHERE customer_state LIKE '%[^A-Za-z]%';
-- QC 6: Detect state codes not in the Brazil list
SELECT DISTINCT
    customer_state
FROM bronze.olist_customers_dataset
WHERE UPPER(TRIM(customer_state)) NOT IN (
    'AC','AL','AP','AM','BA','CE','DF','ES','GO',
    'MA','MT','MS','MG','PA','PB','PR','PE','PI',
    'RJ','RN','RS','RO','RR','SC','SP','SE','TO'
);
-- QC 7: Identify states linked to suspiciously large ZIP diversity
SELECT
    customer_state,
    COUNT(DISTINCT customer_zip_code_prefix) AS distinct_zips
FROM bronze.olist_customers_dataset
GROUP BY customer_state
ORDER BY distinct_zips DESC;
-- QC 8: Frequency distribution for validation
SELECT
    customer_state,
    COUNT(*) AS freq
FROM bronze.olist_customers_dataset
GROUP BY customer_state
ORDER BY freq ASC;

------------------------------------------------------------
-- QC: seller_id – NULL check
------------------------------------------------------------
SELECT 
    SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.olist_sellers_dataset;
------------------------------------------------------------
-- QC: seller_id – duplicate check
------------------------------------------------------------
SELECT 
    seller_id,
    COUNT(*) AS occurrences
FROM bronze.olist_sellers_dataset
GROUP BY seller_id
HAVING COUNT(*) > 1;
------------------------------------------------------------
-- QC: seller_id – invalid length check
------------------------------------------------------------
SELECT 
    seller_id,
    LEN(seller_id) AS length
FROM bronze.olist_sellers_dataset
WHERE LEN(seller_id) <> 32;
------------------------------------------------------------
-- QC: seller_id – invalid character check
------------------------------------------------------------
SELECT 
    seller_id
FROM bronze.olist_sellers_dataset
WHERE seller_id NOT LIKE '%[A-Za-z0-9]%';

------------------------------------------------------------
-- QC: seller_zip_code_prefix – NULL check
------------------------------------------------------------
SELECT 
    SUM(CASE WHEN seller_zip_code_prefix IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.olist_sellers_dataset;
------------------------------------------------------------
-- QC: seller_zip_code_prefix – non-numeric check
------------------------------------------------------------
SELECT 
    seller_zip_code_prefix
FROM bronze.olist_sellers_dataset
WHERE TRY_CAST(seller_zip_code_prefix AS INT) IS NULL;
------------------------------------------------------------
-- QC: seller_zip_code_prefix – invalid length check
------------------------------------------------------------
SELECT 
    seller_zip_code_prefix,
    LEN(seller_zip_code_prefix) AS length
FROM bronze.olist_sellers_dataset
WHERE LEN(seller_zip_code_prefix) <> 5;
------------------------------------------------------------
-- QC: seller_zip_code_prefix – duplicate values check
------------------------------------------------------------
SELECT 
    seller_zip_code_prefix,
    COUNT(*) AS occurrences
FROM bronze.olist_sellers_dataset
GROUP BY seller_zip_code_prefix
HAVING COUNT(*) > 1;
------------------------------------------------------------
-- QC: seller_zip_code_prefix – orphan prefixes vs. geolocation
------------------------------------------------------------
SELECT 
    s.seller_zip_code_prefix
FROM bronze.olist_sellers_dataset s
LEFT JOIN silver.olist_geolocation_dataset g
    ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
WHERE g.geolocation_zip_code_prefix IS NULL;


------------------------------------------------------------
-- QC: seller_city – NULL or empty check
------------------------------------------------------------
SELECT *
FROM bronze.olist_sellers_dataset
WHERE seller_city IS NULL
   OR TRIM(seller_city) = '';
------------------------------------------------------------
-- QC: seller_city – non-alphabetic characters check
------------------------------------------------------------
SELECT *
FROM bronze.olist_sellers_dataset
WHERE seller_city LIKE '%[^a-zA-ZÀ-ÿ ]%';
------------------------------------------------------------
-- QC: seller_city – length check
------------------------------------------------------------
SELECT seller_city, LEN(seller_city) AS length
FROM bronze.olist_sellers_dataset
WHERE LEN(TRIM(seller_city)) > 100;
------------------------------------------------------------
-- QC: seller_city – duplicate city names per ZIP prefix
------------------------------------------------------------
SELECT seller_zip_code_prefix, seller_city, COUNT(*) AS occurrences
FROM bronze.olist_sellers_dataset
GROUP BY seller_zip_code_prefix, seller_city
HAVING COUNT(*) > 1;


------------------------------------------------------------
-- QC: seller_state – NULL or empty check
------------------------------------------------------------
SELECT *
FROM bronze.olist_sellers_dataset
WHERE seller_state IS NULL
   OR TRIM(seller_state) = '';
------------------------------------------------------------
-- QC: seller_state – length check
------------------------------------------------------------
SELECT seller_state, LEN(TRIM(seller_state)) AS length
FROM bronze.olist_sellers_dataset
WHERE LEN(TRIM(seller_state)) <> 2;
------------------------------------------------------------
-- QC: seller_state – non-alphabetic characters check
------------------------------------------------------------
SELECT seller_state
FROM bronze.olist_sellers_dataset
WHERE seller_state LIKE '%[^A-Za-z]%';
------------------------------------------------------------
-- QC: seller_state – duplicate state codes per ZIP prefix
------------------------------------------------------------
SELECT seller_zip_code_prefix, seller_state, COUNT(*) AS occurrences
FROM bronze.olist_sellers_dataset
GROUP BY seller_zip_code_prefix, seller_state
HAVING COUNT(*) > 1;

------------------------------------------------------------
-- 1. Check for NULL or empty product_id values
------------------------------------------------------------
SELECT COUNT(*) AS null_or_empty_count
FROM bronze.olist_products_dataset
WHERE product_id IS NULL
   OR TRIM(product_id) = '';
------------------------------------------------------------
-- 2. Check for duplicate product_id values
------------------------------------------------------------
SELECT product_id, COUNT(*) AS duplicate_count
FROM bronze.olist_products_dataset
GROUP BY product_id
HAVING COUNT(*) > 1;
------------------------------------------------------------
-- 3. Check for product_id format or length issues
-- Olist product_id should usually be a 32-character string
------------------------------------------------------------
SELECT product_id
FROM bronze.olist_products_dataset
WHERE LEN(TRIM(product_id)) <> 32;
------------------------------------------------------------
-- 4. Check for leading/trailing spaces in product_id
------------------------------------------------------------
SELECT product_id
FROM bronze.olist_products_dataset
WHERE product_id <> TRIM(product_id);


------------------------------------------------------------
-- 1. Check for NULL or empty values in product_category_name
------------------------------------------------------------
SELECT COUNT(*) AS null_or_empty_count
FROM bronze.olist_products_dataset
WHERE product_category_name IS NULL
   OR TRIM(product_category_name) = '';
------------------------------------------------------------
-- 2. Check for duplicates within the product_category_name column
-- Not strictly an error, but useful to understand distribution
------------------------------------------------------------
SELECT product_category_name, COUNT(*) AS duplicate_count
FROM bronze.olist_products_dataset
GROUP BY product_category_name
HAVING COUNT(*) > 1;
------------------------------------------------------------
-- 3. Check for leading/trailing spaces
------------------------------------------------------------
SELECT product_category_name
FROM bronze.olist_products_dataset
WHERE product_category_name <> TRIM(product_category_name);
------------------------------------------------------------
-- 4. Check for invalid characters (allowed: lowercase letters, underscores)
------------------------------------------------------------
SELECT product_category_name
FROM bronze.olist_products_dataset
WHERE product_category_name LIKE '%[^a-z_]%';
------------------------------------------------------------
-- 5. Check for inconsistent casing (should be lowercase)
------------------------------------------------------------
SELECT product_category_name
FROM bronze.olist_products_dataset
WHERE product_category_name <> LOWER(product_category_name);
------------------------------------------------------------
-- 6. Check if product_category_name exists in the translation table
-- Ensures referential integrity with silver.product_category_name_translation
------------------------------------------------------------
SELECT DISTINCT p.product_category_name
FROM bronze.olist_products_dataset p
LEFT JOIN silver.product_category_name_translation t
    ON TRIM(p.product_category_name) = TRIM(t.product_category_name)
WHERE t.product_category_name IS NULL;


------------------------------------------------------------
-- 1. Check for NULL values
------------------------------------------------------------
SELECT COUNT(*) AS null_count
FROM bronze.olist_products_dataset
WHERE product_name_lenght IS NULL;
------------------------------------------------------------
-- 2. Check for negative values
------------------------------------------------------------
SELECT product_id, product_name_lenght
FROM bronze.olist_products_dataset
WHERE product_name_lenght < 0;
------------------------------------------------------------
-- 3. Check for zero values
------------------------------------------------------------
SELECT product_id, product_name_lenght
FROM bronze.olist_products_dataset
WHERE product_name_lenght = 0;
------------------------------------------------------------
-- 4. Check for extreme outliers (optional)
-- Assuming normal product names are less than 200 characters
------------------------------------------------------------
SELECT product_id, product_name_lenght
FROM bronze.olist_products_dataset
WHERE product_name_lenght > 200;

------------------------------------------------------------
-- 1. Check for NULL values
------------------------------------------------------------
SELECT COUNT(*) AS null_count
FROM bronze.olist_products_dataset
WHERE product_description_lenght IS NULL;
------------------------------------------------------------
-- 2. Check for negative values
------------------------------------------------------------
SELECT product_id, product_description_lenght
FROM bronze.olist_products_dataset
WHERE product_description_lenght < 0;
------------------------------------------------------------
-- 3. Check for zero values
------------------------------------------------------------
SELECT product_id, product_description_lenght
FROM bronze.olist_products_dataset
WHERE product_description_lenght = 0;
------------------------------------------------------------
-- 4. Check for extreme outliers (optional)
-- Assuming normal product descriptions are less than 2000 characters
------------------------------------------------------------
SELECT product_id, product_description_lenght
FROM bronze.olist_products_dataset
WHERE product_description_lenght > 2000;
------------------------------------------------------------
-- 1. Check for NULL values
------------------------------------------------------------
SELECT COUNT(*) AS null_count
FROM bronze.olist_products_dataset
WHERE product_photos_qty IS NULL;
------------------------------------------------------------
-- 2. Check for negative values
------------------------------------------------------------
SELECT product_id, product_photos_qty
FROM bronze.olist_products_dataset
WHERE product_photos_qty < 0;
------------------------------------------------------------
-- 3. Check for zero values
-- Products with zero photos may be acceptable, but flag them if needed
------------------------------------------------------------
SELECT product_id, product_photos_qty
FROM bronze.olist_products_dataset
WHERE product_photos_qty = 0;
------------------------------------------------------------
-- 4. Check for extreme outliers (optional)
-- Normally a product should not have more than, say, 20 photos
------------------------------------------------------------
SELECT product_id, product_photos_qty
FROM bronze.olist_products_dataset
WHERE product_photos_qty > 20;
------------------------------------------------------------
-- 1. Check for NULL values
------------------------------------------------------------
SELECT COUNT(*) AS null_count
FROM bronze.olist_products_dataset
WHERE product_weight_g IS NULL;
------------------------------------------------------------
-- 2. Check for negative values
------------------------------------------------------------
SELECT product_id, product_weight_g
FROM bronze.olist_products_dataset
WHERE product_weight_g < 0;
------------------------------------------------------------
-- 3. Check for zero values
-- A weight of zero may indicate missing data
------------------------------------------------------------
SELECT product_id, product_weight_g
FROM bronze.olist_products_dataset
WHERE product_weight_g = 0;
------------------------------------------------------------
-- 4. Check for extreme outliers (optional)
-- Reasonable weight range: 1g – 50,000g (50kg)
------------------------------------------------------------
SELECT product_id, product_weight_g
FROM bronze.olist_products_dataset
WHERE product_weight_g > 50000;
------------------------------------------------------------
-- 1. Check for NULL values
------------------------------------------------------------
SELECT COUNT(*) AS null_count
FROM bronze.olist_products_dataset
WHERE product_length_cm IS NULL;
------------------------------------------------------------
-- 2. Check for negative values
------------------------------------------------------------
SELECT product_id, product_length_cm
FROM bronze.olist_products_dataset
WHERE product_length_cm < 0;
------------------------------------------------------------
-- 3. Check for zero values
-- Zero length may indicate missing or invalid data
------------------------------------------------------------
SELECT product_id, product_length_cm
FROM bronze.olist_products_dataset
WHERE product_length_cm = 0;
------------------------------------------------------------
-- 4. Check for extreme outliers (optional)
-- Reasonable product length range: 0.5 cm – 500 cm
------------------------------------------------------------
SELECT product_id, product_length_cm
FROM bronze.olist_products_dataset
WHERE product_length_cm > 500;
------------------------------------------------------------
-- 1. Check for NULL values
------------------------------------------------------------
SELECT COUNT(*) AS null_count
FROM bronze.olist_products_dataset
WHERE product_height_cm IS NULL;
------------------------------------------------------------
-- 2. Check for negative values
------------------------------------------------------------
SELECT product_id, product_height_cm
FROM bronze.olist_products_dataset
WHERE product_height_cm < 0;
------------------------------------------------------------
-- 3. Check for zero values
-- Zero height may indicate missing or invalid data
------------------------------------------------------------
SELECT product_id, product_height_cm
FROM bronze.olist_products_dataset
WHERE product_height_cm = 0;
------------------------------------------------------------
-- 4. Check for extreme outliers (optional)
-- Reasonable product height range: 0.5 cm – 500 cm
------------------------------------------------------------
SELECT product_id, product_height_cm
FROM bronze.olist_products_dataset
WHERE product_height_cm > 500;
------------------------------------------------------------
-- 1. Check for NULL values
------------------------------------------------------------
SELECT COUNT(*) AS null_count
FROM bronze.olist_products_dataset
WHERE product_width_cm IS NULL;
------------------------------------------------------------
-- 2. Check for negative values
------------------------------------------------------------
SELECT product_id, product_width_cm
FROM bronze.olist_products_dataset
WHERE product_width_cm < 0;
------------------------------------------------------------
-- 3. Check for zero values
-- Zero width may indicate missing or invalid data
------------------------------------------------------------
SELECT product_id, product_width_cm
FROM bronze.olist_products_dataset
WHERE product_width_cm = 0;
------------------------------------------------------------
-- 4. Check for extreme outliers (optional)
-- Reasonable product width range: 0.5 cm – 500 cm
------------------------------------------------------------
SELECT product_id, product_width_cm
FROM bronze.olist_products_dataset
WHERE product_width_cm > 500;
/* ============================================================================
   QUALITY CHECKS — bronze.olist_orders_dataset
   ============================================================================ */

-- ============================================================
-- 1. Primary Key: order_id
-- ============================================================

-- Check for NULL or duplicate order_id
-- Expectation: No results
SELECT 
    COUNT(*) AS total_rows,
    COUNT(order_id) AS non_null_order_id,
    COUNT(DISTINCT order_id) AS distinct_order_id,
    COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_order_id
FROM bronze.olist_orders_dataset;

-- Inspect duplicate order_id values (if any)
SELECT 
    order_id,
    COUNT(*) AS occurrences
FROM bronze.olist_orders_dataset
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;

-- ============================================================
-- 2. Foreign Key & Cardinality: customer_id
-- ============================================================

-- Check for NULL customer_id
SELECT COUNT(*) AS null_customer_id
FROM bronze.olist_orders_dataset
WHERE customer_id IS NULL;

-- Check customer_id format consistency (length distribution)
SELECT 
    LEN(customer_id) AS id_length,
    COUNT(*) AS freq
FROM bronze.olist_orders_dataset
GROUP BY LEN(customer_id)
ORDER BY freq DESC;

-- Foreign key consistency: orders without matching customer
SELECT 
    o.customer_id,
    COUNT(*) AS order_count
FROM bronze.olist_orders_dataset o
LEFT JOIN silver.olist_customers_dataset c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL
GROUP BY o.customer_id;

-- Orders per customer (cardinality check)
SELECT 
    customer_id,
    COUNT(*) AS order_count
FROM bronze.olist_orders_dataset
GROUP BY customer_id
ORDER BY order_count DESC;

-- ============================================================
-- 3. Order Status Validation
-- ============================================================

-- Check for NULL order_status
SELECT COUNT(*) AS null_order_status
FROM bronze.olist_orders_dataset
WHERE order_status IS NULL;

-- Status domain & frequency
SELECT 
    order_status,
    COUNT(*) AS freq
FROM bronze.olist_orders_dataset
GROUP BY order_status
ORDER BY freq DESC;

-- Unexpected / invalid order_status values
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_status NOT IN (
    'created', 'approved', 'invoiced', 'processing',
    'shipped', 'delivered', 'canceled', 'unavailable'
);

-- ============================================================
-- 4. Purchase Timestamp Validation
-- ============================================================

-- Check for NULL or future purchase timestamps
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_purchase_timestamp IS NULL
   OR order_purchase_timestamp > GETDATE();

-- Purchase timestamp range
SELECT 
    MIN(order_purchase_timestamp) AS min_purchase_ts,
    MAX(order_purchase_timestamp) AS max_purchase_ts
FROM bronze.olist_orders_dataset;

-- Purchase distribution by year/month
SELECT 
    YEAR(order_purchase_timestamp) AS order_year,
    MONTH(order_purchase_timestamp) AS order_month,
    COUNT(*) AS order_count
FROM bronze.olist_orders_dataset
GROUP BY YEAR(order_purchase_timestamp), MONTH(order_purchase_timestamp)
ORDER BY order_year, order_month;

-- ============================================================
-- 5. Order Approval Timestamp
-- ============================================================

-- Logical inconsistencies or future approvals
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_approved_at IS NOT NULL
  AND (
        order_approved_at < order_purchase_timestamp
     OR order_approved_at > GETDATE()
  );

-- Approval timestamp range
SELECT 
    MIN(order_approved_at) AS min_approved_ts,
    MAX(order_approved_at) AS max_approved_ts
FROM bronze.olist_orders_dataset;

-- ============================================================
-- 6. Delivery to Carrier Timestamp
-- ============================================================

-- Invalid carrier delivery sequencing
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_delivered_carrier_date IS NOT NULL
  AND (
        order_delivered_carrier_date < order_purchase_timestamp
     OR order_delivered_carrier_date < order_approved_at
     OR order_delivered_carrier_date > GETDATE()
  );

-- Carrier delivery range
SELECT 
    MIN(order_delivered_carrier_date) AS min_carrier_ts,
    MAX(order_delivered_carrier_date) AS max_carrier_ts
FROM bronze.olist_orders_dataset;

-- ============================================================
-- 7. Delivery to Customer Timestamp
-- ============================================================

-- Invalid customer delivery sequencing
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_delivered_customer_date IS NOT NULL
  AND (
        order_delivered_customer_date < order_delivered_carrier_date
     OR order_delivered_customer_date < order_purchase_timestamp
     OR order_delivered_customer_date > GETDATE()
  );

-- Customer delivery range
SELECT 
    MIN(order_delivered_customer_date) AS min_customer_ts,
    MAX(order_delivered_customer_date) AS max_customer_ts
FROM bronze.olist_orders_dataset;

-- ============================================================
-- 8. Estimated Delivery Date
-- ============================================================

-- Invalid estimated delivery logic
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_estimated_delivery_date IS NOT NULL
  AND (
        order_estimated_delivery_date < order_purchase_timestamp
     OR order_estimated_delivery_date < order_approved_at
     OR order_estimated_delivery_date > GETDATE()
  );

-- Estimated delivery range
SELECT 
    MIN(order_estimated_delivery_date) AS min_estimated_ts,
    MAX(order_estimated_delivery_date) AS max_estimated_ts
FROM bronze.olist_orders_dataset;

