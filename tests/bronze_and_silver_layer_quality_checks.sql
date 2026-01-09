/*
===============================================================================
Script Name:     bronze_and_silver_layer_quality_checks.sql
===============================================================================
Purpose:
    This script performs comprehensive data quality validations on the 
    Olist e-commerce datasets across the Bronze (staging) and Silver 
    (cleansed) layers.

    The primary objective is to:
      - Assess raw ingested data in the Bronze layer for structural integrity,
        completeness, and basic business rule compliance.
      - Re-validate the transformed and cleansed data in the Silver layer
        to ensure that data quality issues identified in Bronze have been
        properly resolved and that no new issues were introduced during
        transformation.

    The same set of quality checks is executed for both layers by changing
    the target schema reference from `bronze` to `silver`.

    These validations ensure data reliability and prevent the propagation
    of data quality issues into downstream analytical and Gold-layer models.

Target Schemas:
    - bronze  (raw ingestion / staging layer)
    - silver  (cleaned and conformed layer)

Target Tables (Olist E-commerce Datasets):
    - olist_geolocation_dataset
    - olist_customers_dataset
    - product_category_name_translation
    - olist_products_dataset
    - olist_sellers_dataset
    - olist_orders_dataset
    - olist_order_items_dataset
    - olist_order_payments_dataset
    - olist_order_reviews_dataset

Quality Check Coverage:
    - NULL validation on critical business and technical columns
    - Duplicate detection on primary and business keys
    - Referential integrity checks between related entities
    - Data type, format, and domain validation
    - Logical consistency checks for dates, quantities, and monetary values

Usage Notes:
    - Execute this script immediately after raw data ingestion for the
      Bronze layer.
    - Re-run the same script after transformations by switching the schema
      reference to `silver` to validate cleaned data.
    - Any failing checks in Bronze must be resolved before promotion to Silver.
    - Any failing checks in Silver indicate transformation or cleansing issues
      that must be corrected before loading the Gold layer.
    - Script is written and optimized for Microsoft SQL Server.

===============================================================================
*/

-- ============================================================================
-- Test for bronze.olist_geolocation_dataset
-- ============================================================================

-- Check for duplicate geolocation_zip_code_prefix entries
-- This flags ZIP prefixes with more than one row in Bronze
SELECT 
    geolocation_zip_code_prefix,
    COUNT(*) AS duplicate_count
FROM bronze.olist_geolocation_dataset
GROUP BY geolocation_zip_code_prefix
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Check if geolocation_zip_code_prefix contains NULLs
-- This column is required for joining to customers/sellers
SELECT 
    COUNT(*) AS null_zip_prefix_count
FROM bronze.olist_geolocation_dataset
WHERE geolocation_zip_code_prefix IS NULL;

-- Check for empty or whitespace-only zip prefixes
SELECT 
    COUNT(*) AS empty_zip_prefix_count
FROM bronze.olist_geolocation_dataset
WHERE LTRIM(RTRIM(geolocation_zip_code_prefix)) = '';

-- Identify zip prefixes that contain non-numeric characters
-- Only digits (0-9) should be present in this column
SELECT 
    geolocation_zip_code_prefix,
    COUNT(*) AS bad_format_count
FROM bronze.olist_geolocation_dataset
WHERE geolocation_zip_code_prefix NOT LIKE '%[0-9]%'
   OR geolocation_zip_code_prefix LIKE '%[^0-9]%'  -- any non-digit
GROUP BY geolocation_zip_code_prefix;

-- Check invalid zip prefix lengths 
-- Valid length range: 1 to 5 characters
SELECT 
    geolocation_zip_code_prefix,
    LEN(geolocation_zip_code_prefix) AS prefix_length
FROM bronze.olist_geolocation_dataset
WHERE LEN(geolocation_zip_code_prefix) < 1
   OR LEN(geolocation_zip_code_prefix) > 5;

-- Check for zip prefixes mapped to multiple states
-- This indicates potential inconsistent or dirty data
SELECT 
    geolocation_zip_code_prefix,
    COUNT(DISTINCT geolocation_state) AS distinct_state_count
FROM bronze.olist_geolocation_dataset
GROUP BY geolocation_zip_code_prefix
HAVING COUNT(DISTINCT geolocation_state) > 1;

-- Check for zip prefixes mapped to multiple cities
SELECT 
    geolocation_zip_code_prefix,
    COUNT(DISTINCT geolocation_city) AS distinct_city_count
FROM bronze.olist_geolocation_dataset
GROUP BY geolocation_zip_code_prefix
HAVING COUNT(DISTINCT geolocation_city) > 1;

-- Check if geolocation_lat contains NULL values
-- Latitude is required for accurate averaging in the Silver layer
SELECT 
    COUNT(*) AS null_lat_count
FROM bronze.olist_geolocation_dataset
WHERE geolocation_lat IS NULL;

-- Identify non-numeric latitude values
-- Valid latitude must be convertible to a numeric type
SELECT 
    geolocation_lat,
    COUNT(*) AS bad_lat_format_count
FROM bronze.olist_geolocation_dataset
WHERE TRY_CAST(geolocation_lat AS DECIMAL(10,6)) IS NULL
      AND geolocation_lat IS NOT NULL
GROUP BY geolocation_lat;

-- Check for latitude values outside valid range
-- Valid latitude range is: -90 <= lat <= 90
SELECT 
    geolocation_lat,
    COUNT(*) AS out_of_range_count
FROM bronze.olist_geolocation_dataset
WHERE TRY_CAST(geolocation_lat AS DECIMAL(10,6)) < -90
   OR TRY_CAST(geolocation_lat AS DECIMAL(10,6)) > 90
GROUP BY geolocation_lat;

-- Check for outlier latitude values using Z-score logic
-- This helps identify coordinates that are statistically abnormal
WITH lat_stats AS (
    SELECT 
        AVG(CAST(geolocation_lat AS FLOAT)) AS mean_lat,
        STDEV(CAST(geolocation_lat AS FLOAT)) AS std_lat
    FROM bronze.olist_geolocation_dataset
)
SELECT 
    g.geolocation_lat,
    (CAST(g.geolocation_lat AS FLOAT) - s.mean_lat) / NULLIF(s.std_lat, 0) AS z_score
FROM bronze.olist_geolocation_dataset g
CROSS JOIN lat_stats s
WHERE ABS((CAST(g.geolocation_lat AS FLOAT) - s.mean_lat) / NULLIF(s.std_lat, 0)) > 3;

-- Check for duplicated latitude values within the same zip prefix
-- Excessive duplication may indicate imprecise geocoordinates
SELECT 
    geolocation_zip_code_prefix,
    geolocation_lat,
    COUNT(*) AS lat_count
FROM bronze.olist_geolocation_dataset
GROUP BY 
    geolocation_zip_code_prefix,
    geolocation_lat
HAVING COUNT(*) > 1;

-- Check if geolocation_lng contains NULL values
-- Longitude is required for averaging and cannot be NULL
SELECT 
    COUNT(*) AS null_lng_count
FROM bronze.olist_geolocation_dataset
WHERE geolocation_lng IS NULL;

-- Identify non-numeric longitude values
-- Valid longitude must be convertible to a numeric type
SELECT 
    geolocation_lng,
    COUNT(*) AS bad_lng_format_count
FROM bronze.olist_geolocation_dataset
WHERE TRY_CAST(geolocation_lng AS DECIMAL(10,6)) IS NULL
      AND geolocation_lng IS NOT NULL
GROUP BY geolocation_lng;

-- Check for longitude values outside valid range
-- Valid longitude must be between -180 and 180
SELECT 
    geolocation_lng,
    COUNT(*) AS out_of_range_count
FROM bronze.olist_geolocation_dataset
WHERE TRY_CAST(geolocation_lng AS DECIMAL(10,6)) < -180
   OR TRY_CAST(geolocation_lng AS DECIMAL(10,6)) > 180
GROUP BY geolocation_lng;

-- Check for duplicated longitude values within each zip prefix
-- Excessive duplication might suggest low data precision
SELECT 
    geolocation_zip_code_prefix,
    geolocation_lng,
    COUNT(*) AS lng_count
FROM bronze.olist_geolocation_dataset
GROUP BY 
    geolocation_zip_code_prefix,
    geolocation_lng
HAVING COUNT(*) > 1;

-- Check for NULL values in geolocation_city
-- City name should not be NULL because it's part of location enrichment
SELECT 
    COUNT(*) AS null_city_count
FROM bronze.olist_geolocation_dataset
WHERE geolocation_city IS NULL;

-- Check for empty or whitespace-only city names
SELECT 
    COUNT(*) AS empty_city_count
FROM bronze.olist_geolocation_dataset
WHERE LTRIM(RTRIM(geolocation_city)) = '';

-- Identify city names containing invalid characters
-- Valid city names should contain alphabetic characters and spaces only
SELECT 
    geolocation_city,
    COUNT(*) AS problem_count
FROM bronze.olist_geolocation_dataset
WHERE geolocation_city LIKE '%[0-9]%'      -- digits
     OR geolocation_city LIKE '%[^A-Za-zÀ-ÿ ]%'-- non-letter, non-space
GROUP BY geolocation_city;

-- Check for inconsistent casing or trailing spaces
-- Helps determine standardization needs before loading Silver
SELECT 
    geolocation_city,
    LEN(geolocation_city) AS city_length,
    geolocation_city AS raw_city
FROM bronze.olist_geolocation_dataset
WHERE geolocation_city <> UPPER(LTRIM(RTRIM(geolocation_city)));

-- Check for unusually short or long city names
-- Helps identify malformed values
SELECT 
    geolocation_city,
    LEN(geolocation_city) AS city_length
FROM bronze.olist_geolocation_dataset
WHERE LEN(LTRIM(RTRIM(geolocation_city))) < 2
   OR LEN(LTRIM(RTRIM(geolocation_city))) > 40;

-- Check if geolocation_state contains NULL values
-- State should never be NULL because it's part of location identity
SELECT 
    COUNT(*) AS null_state_count
FROM bronze.olist_geolocation_dataset
WHERE geolocation_state IS NULL;

-- Check for empty or whitespace-only state values
SELECT 
    COUNT(*) AS empty_state_count
FROM bronze.olist_geolocation_dataset
WHERE LTRIM(RTRIM(geolocation_state)) = '';

-- Identify invalid state codes
-- Valid state format: exactly 2 alphabetic characters (A–Z)
SELECT 
    geolocation_state,
    COUNT(*) AS invalid_state_count
FROM bronze.olist_geolocation_dataset
WHERE LEN(LTRIM(RTRIM(geolocation_state))) <> 2
   OR geolocation_state LIKE '%[^A-Za-z]%'  -- any non-letter character
GROUP BY geolocation_state;

-- Identify state codes that are not uppercase
-- Helps determine if we need to standardize the field
SELECT 
    geolocation_state
FROM bronze.olist_geolocation_dataset
WHERE geolocation_state <> UPPER(geolocation_state);

-- Check for zip prefixes that map to multiple states
-- This is a critical quality issue because a zip should belong to only one state
SELECT 
    geolocation_zip_code_prefix,
    COUNT(DISTINCT geolocation_state) AS distinct_state_count
FROM bronze.olist_geolocation_dataset
GROUP BY geolocation_zip_code_prefix
HAVING COUNT(DISTINCT geolocation_state) > 1;

-- Frequency distribution of state codes
-- Helps reveal unexpected or rare state abbreviations
SELECT 
    geolocation_state,
    COUNT(*) AS state_count
FROM bronze.olist_geolocation_dataset
GROUP BY geolocation_state
ORDER BY state_count DESC;


-- ============================================================================
-- Test for bronze.olist_customers_dataset
-- ============================================================================
-- Check if customer_id contains NULL values
SELECT 
    COUNT(*) AS null_customer_id_count
FROM bronze.olist_customers_dataset
WHERE customer_id IS NULL;

-- Check for empty or whitespace-only customer_id values
SELECT 
    COUNT(*) AS empty_customer_id_count
FROM bronze.olist_customers_dataset
WHERE LTRIM(RTRIM(customer_id)) = '';

-- Check for duplicated customer_id values
SELECT 
    customer_id,
    COUNT(*) AS occurrences
FROM bronze.olist_customers_dataset
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Check customer_id values that do not follow expected UUID-like format (32 hex chars)
SELECT 
    customer_id
FROM bronze.olist_customers_dataset
WHERE LEN(customer_id) <> 32
   OR customer_id LIKE '%[^0-9A-Fa-f]%';

-- Compare total rows to distinct customer_id count
SELECT
    (SELECT COUNT(*) FROM bronze.olist_customers_dataset) AS total_rows,
    (SELECT COUNT(DISTINCT customer_id) FROM bronze.olist_customers_dataset) AS distinct_customer_ids;

-- Check if customer_unique_id contains NULL values
SELECT 
    COUNT(*) AS null_customer_unique_id_count
FROM bronze.olist_customers_dataset
WHERE customer_unique_id IS NULL;

-- Check for empty or whitespace-only customer_unique_id values
SELECT 
    COUNT(*) AS empty_customer_unique_id_count
FROM bronze.olist_customers_dataset
WHERE LTRIM(RTRIM(customer_unique_id)) = '';

-- Identify customer_unique_id values with invalid length or invalid characters
SELECT
    customer_unique_id
FROM bronze.olist_customers_dataset
WHERE LEN(customer_unique_id) <> 32
   OR customer_unique_id LIKE '%[^0-9A-Fa-f]%';

-- Check how many customer_unique_id values appear more than once
SELECT 
    customer_unique_id,
    COUNT(*) AS occurrences
FROM bronze.olist_customers_dataset
GROUP BY customer_unique_id
HAVING COUNT(*) > 1;

-- Compare total_rows vs distinct customer_unique_id count
SELECT
    (SELECT COUNT(*) FROM bronze.olist_customers_dataset) AS total_rows,
    (SELECT COUNT(DISTINCT customer_unique_id) FROM bronze.olist_customers_dataset) AS distinct_customer_unique_ids;

-- Verify customer_unique_id links to multiple customer_id values (check if unexpected)
SELECT
    customer_unique_id,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids
FROM bronze.olist_customers_dataset
GROUP BY customer_unique_id
HAVING COUNT(DISTINCT customer_id) > 1;

-- Check if customer_zip_code_prefix contains NULL values
SELECT 
    COUNT(*) AS null_zip_prefix_count
FROM bronze.olist_customers_dataset
WHERE customer_zip_code_prefix IS NULL;

-- Identify non-numeric ZIP prefixes
SELECT
    customer_zip_code_prefix
FROM bronze.olist_customers_dataset
WHERE customer_zip_code_prefix NOT LIKE '%[0-9]%'
   OR TRY_CAST(customer_zip_code_prefix AS INT) IS NULL;

-- Identify ZIP prefixes with invalid length
SELECT
    customer_zip_code_prefix,
    LEN(customer_zip_code_prefix) AS prefix_length
FROM bronze.olist_customers_dataset
WHERE LEN(customer_zip_code_prefix) <> 5;

-- Detect ZIP prefixes with fewer than 5 digits (possible missing leading zero)
SELECT
    customer_zip_code_prefix
FROM bronze.olist_customers_dataset
WHERE LEN(customer_zip_code_prefix) < 5;

-- Identify the top/bottom ZIP prefixes by frequency
SELECT
    customer_zip_code_prefix,
    COUNT(*) AS freq
FROM bronze.olist_customers_dataset
GROUP BY customer_zip_code_prefix
ORDER BY freq ASC; -- Use DESC if you want most common first

-- Detect ZIP prefixes that map to multiple customer_state values
SELECT
    customer_zip_code_prefix,
    COUNT(DISTINCT customer_state) AS distinct_states
FROM bronze.olist_customers_dataset
GROUP BY customer_zip_code_prefix
HAVING COUNT(DISTINCT customer_state) > 1;

-- Detect ZIP prefixes that map to multiple customer_city values
SELECT
    customer_zip_code_prefix,
    COUNT(DISTINCT customer_city) AS distinct_cities
FROM bronze.olist_customers_dataset
GROUP BY customer_zip_code_prefix
HAVING COUNT(DISTINCT customer_city) > 1;

-- Check if customer_city contains NULL values
SELECT 
    COUNT(*) AS null_city_count
FROM bronze.olist_customers_dataset
WHERE customer_city IS NULL;

-- Identify blank or whitespace-only values
SELECT 
    COUNT(*) AS blank_city_count
FROM bronze.olist_customers_dataset
WHERE LTRIM(RTRIM(customer_city)) = '';

-- customer_city – non-alphabetic characters check
SELECT *
FROM bronze.olist_customers_dataset
WHERE customer_city LIKE '%[^a-zA-ZÀ-ÿ ]%';

-- Very short city values (likely dirty)
SELECT
    customer_city
FROM bronze.olist_customers_dataset
WHERE LEN(LTRIM(RTRIM(customer_city))) < 3;

-- Identify casing inconsistencies
SELECT 
    customer_city,
    CASE 
        WHEN customer_city = UPPER(customer_city) THEN 'UPPERCASE'
        WHEN customer_city = LOWER(customer_city) THEN 'lowercase'
        ELSE 'Mixed Case'
    END AS casing_type
FROM bronze.olist_customers_dataset
GROUP BY customer_city;

-- Detect cities mapped to >1 state
SELECT
    customer_city,
    COUNT(DISTINCT customer_state) AS distinct_states
FROM bronze.olist_customers_dataset
GROUP BY customer_city
HAVING COUNT(DISTINCT customer_state) > 1;

-- Frequency distribution of cities
SELECT
    customer_city,
    COUNT(*) AS freq
FROM bronze.olist_customers_dataset
GROUP BY customer_city
ORDER BY freq ASC;   -- ASC to see rare/dirty cities first

-- Check if customer_state contains NULL values
SELECT 
    COUNT(*) AS null_state_count
FROM bronze.olist_customers_dataset
WHERE customer_state IS NULL;

-- Identify blank or whitespace-only values
SELECT 
    COUNT(*) AS blank_state_count
FROM bronze.olist_customers_dataset
WHERE LTRIM(RTRIM(customer_state)) = '';

-- Detect invalid length state codes
SELECT 
    customer_state,
    COUNT(*) AS freq
FROM bronze.olist_customers_dataset
WHERE LEN(LTRIM(RTRIM(customer_state))) <> 2
GROUP BY customer_state;

-- Identify inconsistent casing
SELECT
    customer_state,
    CASE 
        WHEN customer_state = UPPER(customer_state) THEN 'UPPERCASE'
        WHEN customer_state = LOWER(customer_state) THEN 'lowercase'
        ELSE 'Mixed Case'
    END AS casing_type
FROM bronze.olist_customers_dataset
GROUP BY customer_state;

-- Detect states containing non-letter characters
SELECT
    customer_state
FROM bronze.olist_customers_dataset
WHERE customer_state LIKE '%[^A-Za-z]%';

-- Detect state codes not in the Brazil list
SELECT DISTINCT
    customer_state
FROM bronze.olist_customers_dataset
WHERE UPPER(TRIM(customer_state)) NOT IN (
    'AC','AL','AP','AM','BA','CE','DF','ES','GO',
    'MA','MT','MS','MG','PA','PB','PR','PE','PI',
    'RJ','RN','RS','RO','RR','SC','SP','SE','TO'
);

-- Identify states linked to suspiciously large ZIP diversity
SELECT
    customer_state,
    COUNT(DISTINCT customer_zip_code_prefix) AS distinct_zips
FROM bronze.olist_customers_dataset
GROUP BY customer_state
ORDER BY distinct_zips DESC;

-- Frequency distribution for validation
SELECT
    customer_state,
    COUNT(*) AS freq
FROM bronze.olist_customers_dataset
GROUP BY customer_state
ORDER BY freq ASC;

-- ============================================================================
-- Test for bronze.product_category_name_translation
-- ============================================================================

-- Check for NULL or empty values in column1
SELECT COUNT(*) AS null_or_empty_count
FROM bronze.product_category_name_translation
WHERE NOT (column1 = 'product_category_name' AND column2 = 'product_category_name_english')
  AND (column1 IS NULL OR TRIM(column1) = '');

-- Check for duplicates in column1
SELECT column1, COUNT(*) AS duplicate_count
FROM bronze.product_category_name_translation
WHERE NOT (column1 = 'product_category_name' AND column2 = 'product_category_name_english')
GROUP BY column1
HAVING COUNT(*) > 1;

-- Check for leading/trailing spaces in column1
SELECT column1
FROM bronze.product_category_name_translation
WHERE NOT (column1 = 'product_category_name' AND column2 = 'product_category_name_english')
  AND column1 <> TRIM(column1);

-- Check for non-ASCII or strange characters
SELECT column1
FROM bronze.product_category_name_translation
WHERE NOT (column1 = 'product_category_name' AND column2 = 'product_category_name_english')
  AND column1 COLLATE Latin1_General_BIN NOT LIKE '%[ -~]%';

-- Check for invalid pattern 
-- Allowed: lowercase letters + underscores only
SELECT column1
FROM bronze.product_category_name_translation
WHERE NOT (column1 = 'product_category_name' AND column2 = 'product_category_name_english')
  AND column1 LIKE '%[^a-z_]%';

-- Check for inconsistent casing
-- Should be lowercase only
SELECT column1
FROM bronze.product_category_name_translation
WHERE NOT (column1 = 'product_category_name' AND column2 = 'product_category_name_english')
  AND column1 <> LOWER(column1);

-- Check for 1-to-many mapping problems.
-- One Portuguese name should map to exactly one English name.
SELECT 
    column1 AS product_category_name,
    COUNT(DISTINCT column2) AS english_variants
FROM bronze.product_category_name_translation
WHERE NOT (column1 = 'product_category_name' AND column2 = 'product_category_name_english')
GROUP BY column1
HAVING COUNT(DISTINCT column2) > 1;

-- Check for NULL or empty values in column2
SELECT COUNT(*) AS null_or_empty_count
FROM bronze.product_category_name_translation
WHERE NOT (column1 = 'product_category_name' 
           AND column2 = 'product_category_name_english')
  AND (column2 IS NULL OR TRIM(column2) = '');

-- Check for duplicates in column2
SELECT column2, COUNT(*) AS duplicate_count
FROM bronze.product_category_name_translation
WHERE NOT (column1 = 'product_category_name' 
           AND column2 = 'product_category_name_english')
GROUP BY column2
HAVING COUNT(*) > 1;

-- Check for leading or trailing spaces in column2
SELECT column2
FROM bronze.product_category_name_translation
WHERE NOT (column1 = 'product_category_name'
           AND column2 = 'product_category_name_english')
  AND column2 <> TRIM(column2);

-- Check for non-ASCII characters (should be English only)
SELECT column2
FROM bronze.product_category_name_translation
WHERE NOT (column1 = 'product_category_name'
           AND column2 = 'product_category_name_english')
  AND column2 COLLATE Latin1_General_BIN NOT LIKE '%[ -~]%';

-- Check for invalid characters
-- Allowed: lowercase letters, underscores, numbers
SELECT column2
FROM bronze.product_category_name_translation
WHERE NOT (column1 = 'product_category_name'
           AND column2 = 'product_category_name_english')
  AND column2 LIKE '%[^a-z0-9_]%';

-- Check for inconsistent casing
-- Should be lowercase only
SELECT column2
FROM bronze.product_category_name_translation
WHERE NOT (column1 = 'product_category_name'
           AND column2 = 'product_category_name_english')
    AND column2 <> LOWER(column2);

-- Check for many-to-one mapping inconsistencies
-- More than one Portuguese value mapping to same English value
SELECT 
    column2 AS product_category_name_english,
    COUNT(DISTINCT column1) AS portuguese_variants
FROM bronze.product_category_name_translation
WHERE NOT (column1 = 'product_category_name'
           AND column2 = 'product_category_name_english')
GROUP BY column2
HAVING COUNT(DISTINCT column1) > 1;

-- ============================================================================
-- Test for bronze.olist_products_dataset
-- ============================================================================

-- Check for NULL or empty product_id values
SELECT COUNT(*) AS null_or_empty_count
FROM bronze.olist_products_dataset
WHERE product_id IS NULL
   OR TRIM(product_id) = '';

-- Check for duplicate product_id values
SELECT product_id, COUNT(*) AS duplicate_count
FROM bronze.olist_products_dataset
GROUP BY product_id
HAVING COUNT(*) > 1;

-- Check for product_id format or length issues
-- Olist product_id should usually be a 32-character string
SELECT product_id
FROM bronze.olist_products_dataset
WHERE LEN(TRIM(product_id)) <> 32;

-- Check for leading/trailing spaces in product_id
SELECT product_id
FROM bronze.olist_products_dataset
WHERE product_id <> TRIM(product_id);

-- Check for NULL or empty values in product_category_name
SELECT COUNT(*) AS null_or_empty_count
FROM bronze.olist_products_dataset
WHERE product_category_name IS NULL
   OR TRIM(product_category_name) = '';

-- Check for duplicates within the product_category_name column
-- Not strictly an error, but useful to understand distribution
SELECT product_category_name, COUNT(*) AS duplicate_count
FROM bronze.olist_products_dataset
GROUP BY product_category_name
HAVING COUNT(*) > 1;

-- Check for leading/trailing spaces
SELECT product_category_name
FROM bronze.olist_products_dataset
WHERE product_category_name <> TRIM(product_category_name);

-- Check for invalid characters (allowed: lowercase letters, underscores)
SELECT product_category_name
FROM bronze.olist_products_dataset
WHERE product_category_name LIKE '%[^a-z_]%';

-- Check for inconsistent casing (should be lowercase)
SELECT product_category_name
FROM bronze.olist_products_dataset
WHERE product_category_name <> LOWER(product_category_name);

-- Check if product_category_name exists in the translation table
-- Ensures referential integrity with silver.product_category_name_translation
SELECT DISTINCT p.product_category_name
FROM bronze.olist_products_dataset p
LEFT JOIN silver.product_category_name_translation t
    ON TRIM(p.product_category_name) = TRIM(t.product_category_name)
WHERE t.product_category_name IS NULL;

-- Check for NULL values
SELECT COUNT(*) AS null_count
FROM bronze.olist_products_dataset
WHERE product_name_lenght IS NULL;

-- Check for negative values
SELECT product_id, product_name_lenght
FROM bronze.olist_products_dataset
WHERE product_name_lenght < 0;

-- Check for zero values
SELECT product_id, product_name_lenght
FROM bronze.olist_products_dataset
WHERE product_name_lenght = 0;

-- Check for extreme outliers (optional)
-- Assuming normal product names are less than 200 characters
SELECT product_id, product_name_lenght
FROM bronze.olist_products_dataset
WHERE product_name_lenght > 200;

-- Check for NULL values
SELECT COUNT(*) AS null_count
FROM bronze.olist_products_dataset
WHERE product_description_lenght IS NULL;

-- Check for negative values
SELECT product_id, product_description_lenght
FROM bronze.olist_products_dataset
WHERE product_description_lenght < 0;

-- Check for zero values
SELECT product_id, product_description_lenght
FROM bronze.olist_products_dataset
WHERE product_description_lenght = 0;

-- Check for extreme outliers (optional)
-- Assuming normal product descriptions are less than 2000 characters
SELECT product_id, product_description_lenght
FROM bronze.olist_products_dataset
WHERE product_description_lenght > 2000;

-- Check for NULL values
SELECT COUNT(*) AS null_count
FROM bronze.olist_products_dataset
WHERE product_photos_qty IS NULL;

-- Check for negative values
SELECT product_id, product_photos_qty
FROM bronze.olist_products_dataset
WHERE product_photos_qty < 0;

-- Check for zero values
-- Products with zero photos may be acceptable, but flag them if needed
SELECT product_id, product_photos_qty
FROM bronze.olist_products_dataset
WHERE product_photos_qty = 0;

-- Check for extreme outliers (optional)
-- Normally a product should not have more than, say, 20 photos
SELECT product_id, product_photos_qty
FROM bronze.olist_products_dataset
WHERE product_photos_qty > 20;

-- Check for NULL values
SELECT COUNT(*) AS null_count
FROM bronze.olist_products_dataset
WHERE product_weight_g IS NULL;

-- Check for negative values
SELECT product_id, product_weight_g
FROM bronze.olist_products_dataset
WHERE product_weight_g < 0;

-- Check for zero values
-- A weight of zero may indicate missing data
SELECT product_id, product_weight_g
FROM bronze.olist_products_dataset
WHERE product_weight_g = 0;

-- Check for extreme outliers (optional)
-- Reasonable weight range: 1g – 50,000g (50kg)
SELECT product_id, product_weight_g
FROM bronze.olist_products_dataset
WHERE product_weight_g > 50000;

-- Check for NULL values
SELECT COUNT(*) AS null_count
FROM bronze.olist_products_dataset
WHERE product_length_cm IS NULL;

-- Check for negative values
SELECT product_id, product_length_cm
FROM bronze.olist_products_dataset
WHERE product_length_cm < 0;

-- Check for zero values
-- Zero length may indicate missing or invalid data
SELECT product_id, product_length_cm
FROM bronze.olist_products_dataset
WHERE product_length_cm = 0;

-- Check for extreme outliers (optional)
-- Reasonable product length range: 0.5 cm – 500 cm
SELECT product_id, product_length_cm
FROM bronze.olist_products_dataset
WHERE product_length_cm > 500;

-- Check for NULL values
SELECT COUNT(*) AS null_count
FROM bronze.olist_products_dataset
WHERE product_height_cm IS NULL;

-- Check for negative values
SELECT product_id, product_height_cm
FROM bronze.olist_products_dataset
WHERE product_height_cm < 0;

-- Check for zero values
-- Zero height may indicate missing or invalid data
SELECT product_id, product_height_cm
FROM bronze.olist_products_dataset
WHERE product_height_cm = 0;

-- Check for extreme outliers (optional)
-- Reasonable product height range: 0.5 cm – 500 cm
SELECT product_id, product_height_cm
FROM bronze.olist_products_dataset
WHERE product_height_cm > 500;

-- Check for NULL values
SELECT COUNT(*) AS null_count
FROM bronze.olist_products_dataset
WHERE product_width_cm IS NULL;

-- Check for negative values
SELECT product_id, product_width_cm
FROM bronze.olist_products_dataset
WHERE product_width_cm < 0;

-- Check for zero values
-- Zero width may indicate missing or invalid data
SELECT product_id, product_width_cm
FROM bronze.olist_products_dataset
WHERE product_width_cm = 0;

-- Check for extreme outliers (optional)
-- Reasonable product width range: 0.5 cm – 500 cm
SELECT product_id, product_width_cm
FROM bronze.olist_products_dataset
WHERE product_width_cm > 500;

-- ============================================================================
-- Test for bronze.olist_sellers_dataset
-- ============================================================================

-- seller_id – NULL check
SELECT 
    SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.olist_sellers_dataset;

-- seller_id – duplicate check
SELECT 
    seller_id,
    COUNT(*) AS occurrences
FROM bronze.olist_sellers_dataset
GROUP BY seller_id
HAVING COUNT(*) > 1;

-- seller_id – invalid length check
SELECT 
    seller_id,
    LEN(seller_id) AS length
FROM bronze.olist_sellers_dataset
WHERE LEN(seller_id) <> 32;

-- seller_id – invalid character check
SELECT 
    seller_id
FROM bronze.olist_sellers_dataset
WHERE seller_id NOT LIKE '%[A-Za-z0-9]%';

-- seller_zip_code_prefix – NULL check
SELECT 
    SUM(CASE WHEN seller_zip_code_prefix IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.olist_sellers_dataset;

-- seller_zip_code_prefix – non-numeric check
SELECT 
    seller_zip_code_prefix
FROM bronze.olist_sellers_dataset
WHERE TRY_CAST(seller_zip_code_prefix AS INT) IS NULL;

-- seller_zip_code_prefix – invalid length check
SELECT 
    seller_zip_code_prefix,
    LEN(seller_zip_code_prefix) AS length
FROM bronze.olist_sellers_dataset
WHERE LEN(seller_zip_code_prefix) <> 5;

-- seller_zip_code_prefix – duplicate values check
SELECT 
    seller_zip_code_prefix,
    COUNT(*) AS occurrences
FROM bronze.olist_sellers_dataset
GROUP BY seller_zip_code_prefix
HAVING COUNT(*) > 1;

-- seller_zip_code_prefix – orphan prefixes vs. geolocation
SELECT 
    s.seller_zip_code_prefix
FROM bronze.olist_sellers_dataset s
LEFT JOIN silver.olist_geolocation_dataset g
    ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
WHERE g.geolocation_zip_code_prefix IS NULL;

-- seller_city – NULL or empty check
SELECT *
FROM bronze.olist_sellers_dataset
WHERE seller_city IS NULL
   OR TRIM(seller_city) = '';

-- seller_city – non-alphabetic characters check
SELECT *
FROM bronze.olist_sellers_dataset
WHERE seller_city LIKE '%[^a-zA-ZÀ-ÿ ]%';

-- seller_city – length check
SELECT seller_city, LEN(seller_city) AS length
FROM bronze.olist_sellers_dataset
WHERE LEN(TRIM(seller_city)) > 100;

-- seller_city – duplicate city names per ZIP prefix
SELECT seller_zip_code_prefix, seller_city, COUNT(*) AS occurrences
FROM bronze.olist_sellers_dataset
GROUP BY seller_zip_code_prefix, seller_city
HAVING COUNT(*) > 1;

-- seller_state – NULL or empty check
SELECT *
FROM bronze.olist_sellers_dataset
WHERE seller_state IS NULL
   OR TRIM(seller_state) = '';

-- seller_state – length check
SELECT seller_state, LEN(TRIM(seller_state)) AS length
FROM bronze.olist_sellers_dataset
WHERE LEN(TRIM(seller_state)) <> 2;

-- seller_state – non-alphabetic characters check
SELECT seller_state
FROM bronze.olist_sellers_dataset
WHERE seller_state LIKE '%[^A-Za-z]%';

-- seller_state – duplicate state codes per ZIP prefix
SELECT seller_zip_code_prefix, seller_state, COUNT(*) AS occurrences
FROM bronze.olist_sellers_dataset
GROUP BY seller_zip_code_prefix, seller_state
HAVING COUNT(*) > 1;

-- ============================================================================
-- Test for bronze.olist_orders_dataset
-- ============================================================================

-- Check for NULL order_id
SELECT COUNT(*) AS null_order_id
FROM bronze.olist_orders_dataset
WHERE order_id IS NULL;

-- Check for duplicate order_id
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS distinct_order_id,
    COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_count
FROM bronze.olist_orders_dataset;

-- Inspect duplicate order_id if they exist
SELECT 
    order_id,
    COUNT(*) AS occurrences
FROM bronze.olist_orders_dataset
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;

-- Check for NULL customer_id
SELECT COUNT(*) AS null_customer_id
FROM bronze.olist_orders_dataset
WHERE customer_id IS NULL;

-- Check customer_id format consistency (length patterns)
SELECT 
    LEN(customer_id) AS id_length,
    COUNT(*) AS freq
FROM bronze.olist_orders_dataset
GROUP BY LEN(customer_id)
ORDER BY freq DESC;

-- Check if customer_id exists in customers table
-- (foreign key consistency check)
SELECT 
    o.customer_id,
    COUNT(*) AS order_count
FROM bronze.olist_orders_dataset o
LEFT JOIN silver.olist_customers_dataset c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL
GROUP BY o.customer_id;

-- Frequency distribution of orders per customer
SELECT 
    customer_id,
    COUNT(*) AS order_count
FROM bronze.olist_orders_dataset
GROUP BY customer_id
ORDER BY order_count DESC;

-- Check for NULL order_status
SELECT COUNT(*) AS null_order_status
FROM bronze.olist_orders_dataset
WHERE order_status IS NULL;

-- List distinct order_status values
SELECT DISTINCT order_status
FROM bronze.olist_orders_dataset
ORDER BY order_status;

-- Frequency distribution of each order_status
SELECT 
    order_status,
    COUNT(*) AS freq
FROM bronze.olist_orders_dataset
GROUP BY order_status
ORDER BY freq DESC;

-- Check for unusual or inconsistent status values
-- (e.g., typos or unexpected statuses)
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_status NOT IN (
    'created', 'approved', 'invoiced', 'processing', 
    'shipped', 'delivered', 'canceled', 'unavailable'
);

-- Check for NULL order_purchase_timestamp
SELECT COUNT(*) AS null_order_purchase_timestamp
FROM bronze.olist_orders_dataset
WHERE order_purchase_timestamp IS NULL;

-- Check for invalid or future dates
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_purchase_timestamp > GETDATE();  -- future dates

-- Get min and max order_purchase_timestamp
SELECT 
    MIN(order_purchase_timestamp) AS min_purchase_ts,
    MAX(order_purchase_timestamp) AS max_purchase_ts
FROM bronze.olist_orders_dataset;

-- Count orders per year/month for distribution
SELECT 
    YEAR(order_purchase_timestamp) AS order_year,
    MONTH(order_purchase_timestamp) AS order_month,
    COUNT(*) AS order_count
FROM bronze.olist_orders_dataset
GROUP BY YEAR(order_purchase_timestamp), MONTH(order_purchase_timestamp)
ORDER BY order_year, order_month;

-- Check for NULL order_approved_at
-- Note: Orders can exist without approval yet, so NULLs are possible
SELECT COUNT(*) AS null_order_approved
FROM bronze.olist_orders_dataset
WHERE order_approved_at IS NULL;

-- 2. Check for order_approved_at before order_purchase_timestamp
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_approved_at < order_purchase_timestamp;

-- Check for future order_approved_at
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_approved_at > GETDATE();

-- Get min and max order_approved_at
SELECT 
    MIN(order_approved_at) AS min_approved_ts,
    MAX(order_approved_at) AS max_approved_ts
FROM bronze.olist_orders_dataset;

-- Distribution of approved orders per year/month
SELECT 
    YEAR(order_approved_at) AS approved_year,
    MONTH(order_approved_at) AS approved_month,
    COUNT(*) AS approved_count
FROM bronze.olist_orders_dataset
GROUP BY YEAR(order_approved_at), MONTH(order_approved_at)
ORDER BY approved_year, approved_month;

-- Check for NULL order_delivered_carrier_date
-- Note: Some orders may still be in transit or canceled
SELECT COUNT(*) AS null_delivered_carrier
FROM bronze.olist_orders_dataset
WHERE order_delivered_carrier_date IS NULL;

-- Check for delivery to carrier before order approval
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_delivered_carrier_date < order_approved_at;

-- Check for delivery to carrier before purchase
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_delivered_carrier_date < order_purchase_timestamp;

-- Check for future delivery to carrier date
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_delivered_carrier_date > GETDATE();

-- Get min and max delivery to carrier date
SELECT 
    MIN(order_delivered_carrier_date) AS min_carrier_ts,
    MAX(order_delivered_carrier_date) AS max_carrier_ts
FROM bronze.olist_orders_dataset;

-- Distribution of deliveries to carrier per year/month
SELECT 
    YEAR(order_delivered_carrier_date) AS carrier_year,
    MONTH(order_delivered_carrier_date) AS carrier_month,
    COUNT(*) AS delivery_count
FROM bronze.olist_orders_dataset
GROUP BY YEAR(order_delivered_carrier_date), MONTH(order_delivered_carrier_date)
ORDER BY carrier_year, carrier_month;

-- Check for NULL order_delivered_customer_date
-- Note: Orders may not have been delivered yet
SELECT COUNT(*) AS null_delivered_customer
FROM bronze.olist_orders_dataset
WHERE order_delivered_customer_date IS NULL;

-- Check for delivery to customer before delivery to carrier
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_delivered_customer_date < order_delivered_carrier_date;

-- Check for delivery to customer before purchase
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_delivered_customer_date < order_purchase_timestamp;

-- Check for delivery to customer before carrier
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_delivered_customer_date < order_delivered_carrier_date;

-- Check for future delivery to customer date
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_delivered_customer_date > GETDATE();

-- Get min and max delivery to customer date
SELECT 
    MIN(order_delivered_customer_date) AS min_customer_ts,
    MAX(order_delivered_customer_date) AS max_customer_ts
FROM bronze.olist_orders_dataset;

-- Distribution of deliveries to customer per year/month
SELECT 
    YEAR(order_delivered_customer_date) AS customer_year,
    MONTH(order_delivered_customer_date) AS customer_month,
    COUNT(*) AS delivery_count
FROM bronze.olist_orders_dataset
GROUP BY YEAR(order_delivered_customer_date), MONTH(order_delivered_customer_date)
ORDER BY customer_year, customer_month;

-- Check for NULL estimated delivery dates
SELECT COUNT(*) AS null_estimated_delivery
FROM bronze.olist_orders_dataset
WHERE order_estimated_delivery_date IS NULL;

-- Check for estimated delivery before purchase
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_estimated_delivery_date < order_purchase_timestamp;

-- Check for estimated delivery before order approval
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_estimated_delivery_date < order_approved_at;

-- Check for future estimated delivery dates
SELECT *
FROM bronze.olist_orders_dataset
WHERE order_estimated_delivery_date > GETDATE();

-- Min and max estimated delivery dates
SELECT 
    MIN(order_estimated_delivery_date) AS min_estimated_ts,
    MAX(order_estimated_delivery_date) AS max_estimated_ts
FROM bronze.olist_orders_dataset;

-- Distribution of estimated deliveries per year/month
SELECT 
    YEAR(order_estimated_delivery_date) AS est_year,
    MONTH(order_estimated_delivery_date) AS est_month,
    COUNT(*) AS est_count
FROM bronze.olist_orders_dataset
GROUP BY YEAR(order_estimated_delivery_date), MONTH(order_estimated_delivery_date)
ORDER BY est_year, est_month;

-- ============================================================================
-- Test for bronze.olist_order_items_dataset
-- ============================================================================

-- Count total rows
SELECT COUNT(*) AS total_rows
FROM bronze.olist_order_items_dataset;

-- Count distinct order IDs
SELECT COUNT(DISTINCT order_id) AS distinct_order_ids
FROM bronze.olist_order_items_dataset;

-- Detect NULL order_id
SELECT COUNT(*) AS null_order_ids
FROM bronze.olist_order_items_dataset
WHERE order_id IS NULL;

-- Detect empty string order_id
SELECT COUNT(*) AS empty_order_ids
FROM bronze.olist_order_items_dataset
WHERE LTRIM(RTRIM(order_id)) = '';

-- Check if order_id links to orders table (FK check)
SELECT oi.order_id
FROM bronze.olist_order_items_dataset oi
LEFT JOIN bronze.olist_orders_dataset o
    ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

-- NULL order_item_id
SELECT COUNT(*) AS null_order_item_id
FROM bronze.olist_order_items_dataset
WHERE order_item_id IS NULL;

-- order_item_id must be >= 1
SELECT COUNT(*) AS invalid_item_ids
FROM bronze.olist_order_items_dataset
WHERE order_item_id < 1;

-- Detect duplicates (order_id + order_item_id must be unique)
SELECT order_id, order_item_id, COUNT(*) AS cnt
FROM bronze.olist_order_items_dataset
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1;

-- Verify sequential numbering (no gaps, no jumps)
;WITH c AS (
    SELECT
        order_id,
        order_item_id,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_item_id) AS rn
    FROM bronze.olist_order_items_dataset
)
SELECT order_id, order_item_id, rn
FROM c
WHERE order_item_id <> rn;

-- For each order, check min(order_item_id) should be 1
SELECT order_id, MIN(order_item_id) AS min_item_id
FROM bronze.olist_order_items_dataset
GROUP BY order_id
HAVING MIN(order_item_id) <> 1;

-- Detect gaps (e.g., items 1, 3 exist but 2 missing)
SELECT order_id
FROM (
    SELECT
        order_id,
        order_item_id,
        LEAD(order_item_id) OVER (PARTITION BY order_id ORDER BY order_item_id) AS next_item
    FROM bronze.olist_order_items_dataset
) x
WHERE next_item IS NOT NULL
  AND next_item <> order_item_id + 1;

-- NULL product_id
SELECT COUNT(*) AS null_product_id
FROM bronze.olist_order_items_dataset
WHERE product_id IS NULL;

-- Empty string product_id (just in case)
SELECT COUNT(*) AS empty_string_product_id
FROM bronze.olist_order_items_dataset
WHERE product_id = '';

-- product_id length distribution
SELECT
    MIN(LEN(product_id)) AS min_len,
    MAX(LEN(product_id)) AS max_len,
    AVG(LEN(product_id)) AS avg_len
FROM bronze.olist_order_items_dataset;

-- Detect non-alphanumeric product IDs
SELECT product_id
FROM bronze.olist_order_items_dataset
WHERE product_id LIKE '%[^A-Za-z0-9]%'
GROUP BY product_id;

-- Duplicates are allowed (same product sold many times),  
-- but we want to check for weird patterns
SELECT product_id, COUNT(*) AS cnt
FROM bronze.olist_order_items_dataset
GROUP BY product_id
ORDER BY cnt DESC;

-- product_id that do NOT exist in the products table (orphan IDs)
SELECT oi.product_id
FROM bronze.olist_order_items_dataset oi
LEFT JOIN bronze.olist_products_dataset p
    ON oi.product_id = p.product_id
WHERE p.product_id IS NULL
GROUP BY oi.product_id;

-- NULL seller_id
SELECT COUNT(*) AS null_seller_id
FROM bronze.olist_order_items_dataset
WHERE seller_id IS NULL;

-- Empty string seller_id
SELECT COUNT(*) AS empty_string_seller_id
FROM bronze.olist_order_items_dataset
WHERE seller_id = '';

-- Length distribution of seller_id
SELECT
    MIN(LEN(seller_id)) AS min_len,
    MAX(LEN(seller_id)) AS max_len,
    AVG(LEN(seller_id)) AS avg_len
FROM bronze.olist_order_items_dataset;

-- Detect any non-alphanumeric characters
SELECT seller_id
FROM bronze.olist_order_items_dataset
WHERE seller_id LIKE '%[^A-Za-z0-9]%'
GROUP BY seller_id;

-- Frequency check (to spot unusual patterns)
SELECT seller_id, COUNT(*) AS cnt
FROM bronze.olist_order_items_dataset
GROUP BY seller_id
ORDER BY cnt DESC;

-- seller_id not found in sellers table (orphan sellers)
SELECT oi.seller_id
FROM bronze.olist_order_items_dataset oi
LEFT JOIN bronze.olist_sellers_dataset s
    ON oi.seller_id = s.seller_id
WHERE s.seller_id IS NULL
GROUP BY oi.seller_id;

-- NULL shipping_limit_date
SELECT COUNT(*) AS null_shipping_limit_date
FROM bronze.olist_order_items_dataset
WHERE shipping_limit_date IS NULL;

-- Invalid or out-of-range years
SELECT
    MIN(shipping_limit_date) AS min_date,
    MAX(shipping_limit_date) AS max_date
FROM bronze.olist_order_items_dataset;

-- Check for impossible dates (very old or too futuristic)
SELECT shipping_limit_date
FROM bronze.olist_order_items_dataset
WHERE YEAR(shipping_limit_date) NOT BETWEEN 2016 AND 2020
ORDER BY shipping_limit_date;

-- Compare shipping limit vs order purchase timestamp
-- Should NOT ship before order date
SELECT oi.order_id, oi.shipping_limit_date, o.order_purchase_timestamp
FROM bronze.olist_order_items_dataset oi
JOIN silver.olist_orders_dataset o
    ON oi.order_id = o.order_id
WHERE oi.shipping_limit_date < o.order_purchase_timestamp;

-- Duplicates? (allowed, but useful to check)
SELECT shipping_limit_date, COUNT(*) AS cnt
FROM bronze.olist_order_items_dataset
GROUP BY shipping_limit_date
ORDER BY cnt DESC;

-- NULL values
SELECT COUNT(*) AS null_price
FROM bronze.olist_order_items_dataset
WHERE price IS NULL;

-- Zero or negative prices
SELECT COUNT(*) AS non_positive_prices
FROM bronze.olist_order_items_dataset
WHERE price <= 0;

-- Price distribution (basic statistics)
SELECT
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    AVG(price) AS avg_price
FROM bronze.olist_order_items_dataset;

-- Outlier search (prices unusually high)
SELECT *
FROM bronze.olist_order_items_dataset
WHERE price > 5000
ORDER BY price DESC;

-- Ensure price is consistent per product (optional but insightful)
SELECT product_id,
       COUNT(*) AS rows_per_product,
       MIN(price) AS min_price,
       MAX(price) AS max_price
FROM bronze.olist_order_items_dataset
GROUP BY product_id
HAVING MIN(price) <> MAX(price)   -- inconsistent prices
ORDER BY rows_per_product DESC;

-- NULL values
SELECT COUNT(*) AS null_freight_value
FROM bronze.olist_order_items_dataset
WHERE freight_value IS NULL;

-- Negative freight
SELECT COUNT(*) AS negative_freight
FROM bronze.olist_order_items_dataset
WHERE freight_value < 0;

-- Distribution (basic stats)
SELECT
    MIN(freight_value) AS min_freight,
    MAX(freight_value) AS max_freight,
    AVG(freight_value) AS avg_freight
FROM bronze.olist_order_items_dataset;

-- High freight cost outliers
SELECT *
FROM bronze.olist_order_items_dataset
WHERE freight_value > 500
ORDER BY freight_value DESC;

-- Freight consistency per order
SELECT order_id,
       COUNT(*) AS item_count,
       SUM(freight_value) AS total_freight,
       MAX(freight_value) AS max_item_freight
FROM bronze.olist_order_items_dataset
GROUP BY order_id
ORDER BY total_freight DESC;

-- ============================================================================
-- Test for bronze.olist_order_payments_dataset
-- ============================================================================

-- Check for NULL order_id (should not exist)
SELECT *
FROM bronze.olist_order_payments_dataset
WHERE order_id IS NULL;

-- Check order_id length (Olist uses 32-character IDs)
SELECT order_id, LEN(order_id) AS length
FROM bronze.olist_order_payments_dataset
WHERE LEN(order_id) <> 32;

-- Check for invalid characters 
-- (order_id should contain only lowercase letters and numbers)
SELECT order_id
FROM bronze.olist_order_payments_dataset
WHERE order_id LIKE '%[^a-z0-9]%';

-- Check for duplicate payment rows 
-- (order_id + payment_sequential must be unique)
SELECT 
    order_id,
    payment_sequential,
    COUNT(*) AS cnt
FROM bronze.olist_order_payments_dataset
GROUP BY order_id, payment_sequential
HAVING COUNT(*) > 1;

-- Check referential integrity 
-- Find payment records whose order_id does not exist in bronze orders table
SELECT p.order_id
FROM bronze.olist_order_payments_dataset p
LEFT JOIN bronze.olist_orders_dataset o 
    ON p.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Summary check 
-- Compare total rows vs distinct order_ids
SELECT 
    COUNT(*) AS total_payment_rows,
    COUNT(DISTINCT order_id) AS distinct_order_ids
FROM bronze.olist_order_payments_dataset;

-- Check for duplicate payment rows 
SELECT 
    order_id,
    payment_sequential,
    COUNT(*) AS cnt
FROM bronze.olist_order_payments_dataset
GROUP BY order_id, payment_sequential
HAVING COUNT(*) > 1;

-- Check for NULL values in payment_sequential
SELECT COUNT(*) AS null_count
FROM bronze.olist_order_payments_dataset
WHERE payment_sequential IS NULL;

-- payment_sequential should never be negative
SELECT COUNT(*) AS negative_values
FROM bronze.olist_order_payments_dataset
WHERE payment_sequential < 0;

-- Validate that each order_id starts payment_sequential at 1
SELECT order_id, MIN(payment_sequential) AS min_sequence
FROM bronze.olist_order_payments_dataset
GROUP BY order_id
HAVING MIN(payment_sequential) <> 1;

-- Check for sequence gaps (e.g., 1, 3 but missing 2)
WITH seq AS (
    SELECT 
        order_id,
        payment_sequential,
        LAG(payment_sequential) OVER (PARTITION BY order_id ORDER BY payment_sequential) AS prev_seq
    FROM bronze.olist_order_payments_dataset
)
SELECT order_id, payment_sequential AS current_seq, prev_seq
FROM seq
WHERE prev_seq IS NOT NULL
  AND payment_sequential <> prev_seq + 1;

-- Check for duplicate sequence numbers within the same order_id
SELECT order_id, payment_sequential, COUNT(*) AS duplicate_count
FROM bronze.olist_order_payments_dataset
GROUP BY order_id, payment_sequential
HAVING COUNT(*) > 1;

-- Validate payment_sequential is always an integer
SELECT COUNT(*) AS non_integer_values
FROM bronze.olist_order_payments_dataset
WHERE payment_sequential <> FLOOR(payment_sequential);

-- Check for NULL values in payment_type
SELECT COUNT(*) AS null_count
FROM bronze.olist_order_payments_dataset
WHERE payment_type IS NULL;

-- Check for empty or whitespace values in payment_type
SELECT COUNT(*) AS empty_or_whitespace
FROM bronze.olist_order_payments_dataset
WHERE TRIM(payment_type) = '';

-- Inspect distinct payment types to identify unexpected categories
SELECT DISTINCT payment_type
FROM bronze.olist_order_payments_dataset
ORDER BY payment_type;

-- Count records with unexpected payment types
SELECT COUNT(*) AS unexpected_payment_methods
FROM bronze.olist_order_payments_dataset
WHERE payment_type NOT IN (
    'credit_card',
    'debit_card',
    'boleto',
    'voucher',
    'not_defined',
    'paypal'
);

-- Get total count by payment type
SELECT payment_type, COUNT(*) AS count_per_type
FROM bronze.olist_order_payments_dataset
GROUP BY payment_type
ORDER BY count_per_type DESC;

-- Check for uppercase, mixed-case, or inconsistent casing
SELECT COUNT(*) AS inconsistent_case_count
FROM bronze.olist_order_payments_dataset
WHERE payment_type <> LOWER(payment_type);

-- Detect values with leading or trailing spaces
SELECT COUNT(*) AS spaced_values
FROM bronze.olist_order_payments_dataset
WHERE payment_type <> TRIM(payment_type);

-- Check for NULL values in payment_installments
SELECT COUNT(*) AS null_count
FROM bronze.olist_order_payments_dataset
WHERE payment_installments IS NULL;

-- Check for negative installment counts
SELECT COUNT(*) AS negative_values
FROM bronze.olist_order_payments_dataset
WHERE payment_installments < 0;

-- Check for unusually high installment counts (> 24)
SELECT COUNT(*) AS unusually_high_installments
FROM bronze.olist_order_payments_dataset
WHERE payment_installments > 24;

-- Distribution of installment counts
SELECT payment_installments, COUNT(*) AS count_per_value
FROM bronze.olist_order_payments_dataset
GROUP BY payment_installments
ORDER BY payment_installments;

-- Check inconsistencies between payment_type and payment_installments
SELECT *
FROM bronze.olist_order_payments_dataset
WHERE 
    (payment_type = 'debit_card' AND payment_installments <> 1)
    OR (payment_type = 'credit_card' AND payment_installments < 1)
    OR (payment_type IN ('boleto', 'voucher') AND payment_installments > 1);

-- Check for fractional installments (should not happen)
SELECT COUNT(*) AS fractional_values
FROM bronze.olist_order_payments_dataset
WHERE payment_installments <> FLOOR(payment_installments);

-- Check for NULL values in payment_value
SELECT COUNT(*) AS null_count
FROM bronze.olist_order_payments_dataset
WHERE payment_value IS NULL;

-- Check for negative payment amounts
SELECT COUNT(*) AS negative_values
FROM bronze.olist_order_payments_dataset
WHERE payment_value < 0;

-- Check for zero payment amounts
SELECT COUNT(*) AS zero_values
FROM bronze.olist_order_payments_dataset
WHERE payment_value = 0;

-- Check for unusually high payment amounts (e.g., > 10,000 BRL)
SELECT *
FROM bronze.olist_order_payments_dataset
WHERE payment_value > 10000
ORDER BY payment_value DESC;

-- Example check: sum of payments per order
SELECT order_id, SUM(payment_value) AS total_payments
FROM bronze.olist_order_payments_dataset
GROUP BY order_id
HAVING SUM(payment_value) <= 0;

-- Check for non-numeric entries (should not exist)
SELECT *
FROM bronze.olist_order_payments_dataset
WHERE ISNUMERIC(payment_value) = 0;

-- ============================================================================
-- Test for bronze.olist_order_reviews_dataset
-- ============================================================================

-- Total number of rows
SELECT COUNT(*) AS total_rows
FROM bronze.olist_order_reviews_dataset;

-- Number of distinct review_ids
SELECT COUNT(DISTINCT review_id) AS distinct_review_ids
FROM bronze.olist_order_reviews_dataset;

-- Check for NULLs
SELECT COUNT(*) AS null_review_ids
FROM bronze.olist_order_reviews_dataset
WHERE review_id IS NULL;

-- Check for duplicates
SELECT review_id, COUNT(*) AS cnt
FROM bronze.olist_order_reviews_dataset
GROUP BY review_id
HAVING COUNT(*) > 1;

-- Number of distinct order_ids
SELECT COUNT(DISTINCT order_id) AS distinct_order_ids
FROM bronze.olist_order_reviews_dataset;

-- Check for NULLs
SELECT COUNT(*) AS null_order_ids
FROM bronze.olist_order_reviews_dataset
WHERE order_id IS NULL;

-- Check for duplicates (in case multiple reviews exist per order)
SELECT order_id, COUNT(*) AS cnt
FROM bronze.olist_order_reviews_dataset
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Check for NULL values
SELECT COUNT(*) AS null_review_score
FROM bronze.olist_order_reviews_dataset
WHERE review_score IS NULL;

-- Check for valid range of scores (assuming 1 to 5)
SELECT review_score, COUNT(*) AS cnt
FROM bronze.olist_order_reviews_dataset
GROUP BY review_score
ORDER BY review_score;

-- Check for duplicates (if needed in context)
SELECT review_score, COUNT(*) AS cnt
FROM bronze.olist_order_reviews_dataset
GROUP BY review_score
HAVING COUNT(*) > 1;

-- Check for NULL values
SELECT COUNT(*) AS null_review_comment_title
FROM bronze.olist_order_reviews_dataset
WHERE review_comment_title IS NULL;

-- Check for empty strings
SELECT COUNT(*) AS empty_review_comment_title
FROM bronze.olist_order_reviews_dataset
WHERE LTRIM(RTRIM(review_comment_title)) = '';

-- Check for maximum length (optional, to see if it exceeds expected size)
SELECT MAX(LEN(review_comment_title)) AS max_length
FROM bronze.olist_order_reviews_dataset;

-- Sample some distinct values (optional)
SELECT DISTINCT TOP 20 review_comment_title
FROM bronze.olist_order_reviews_dataset;

-- Check for NULL values
SELECT COUNT(*) AS null_review_comment_message
FROM bronze.olist_order_reviews_dataset
WHERE review_comment_message IS NULL;

-- Check for empty strings
SELECT COUNT(*) AS empty_review_comment_message
FROM bronze.olist_order_reviews_dataset
WHERE LTRIM(RTRIM(review_comment_message)) = '';

-- Check for unusually long messages
SELECT MAX(LEN(review_comment_message)) AS max_length,
       AVG(LEN(review_comment_message)) AS avg_length
FROM bronze.olist_order_reviews_dataset;

-- Optionally, check for messages with only special characters or numbers
SELECT COUNT(*) AS non_text_messages
FROM bronze.olist_order_reviews_dataset
WHERE review_comment_message NOT LIKE '%[A-Za-z]%';

-- Check for NULL values
SELECT COUNT(*) AS null_review_creation_date
FROM bronze.olist_order_reviews_dataset
WHERE review_creation_date IS NULL;

-- Check for invalid dates (if stored as string, try conversion)
SELECT *
FROM bronze.olist_order_reviews_dataset
WHERE ISDATE(review_creation_date) = 0;

-- Check for invalid or out-of-range dates
SELECT COUNT(*) AS invalid_dates
FROM bronze.olist_order_reviews_dataset
WHERE review_creation_date < '2016-01-01'
   OR review_creation_date > GETDATE();

-- Check for earliest and latest date
SELECT MIN(review_creation_date) AS min_date, MAX(review_creation_date) AS max_date
FROM bronze.olist_order_reviews_dataset;

-- Check for duplicates if review_id is not unique
SELECT review_creation_date, COUNT(*) AS cnt
FROM bronze.olist_order_reviews_dataset
GROUP BY review_creation_date
HAVING COUNT(*) > 1;

-- Check for NULL values (reviews might not have answers yet)
SELECT COUNT(*) AS null_review_answer
FROM bronze.olist_order_reviews_dataset
WHERE review_answer_timestamp IS NULL;

-- Check for dates before the review was created (should not happen)
SELECT COUNT(*) AS invalid_answer_dates
FROM bronze.olist_order_reviews_dataset
WHERE review_answer_timestamp < review_creation_date;

-- Check for future dates (after today)
SELECT COUNT(*) AS future_answer_dates
FROM bronze.olist_order_reviews_dataset
WHERE review_answer_timestamp > GETDATE();

-- Check for duplicates if review_id is not unique
SELECT review_answer_timestamp, COUNT(*) AS cnt
FROM bronze.olist_order_reviews_dataset
GROUP BY review_answer_timestamp
HAVING COUNT(*) > 1;
