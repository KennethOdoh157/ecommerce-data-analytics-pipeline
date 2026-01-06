/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/

-------------------------------
-- 1. olist_customers_dataset
-------------------------------
IF OBJECT_ID('silver.olist_customers_dataset', 'U') IS NOT NULL
    DROP TABLE silver.olist_customers_dataset;
GO

CREATE TABLE silver.olist_customers_dataset (
    customer_id                VARCHAR(50) NULL,
    customer_unique_id         VARCHAR(50) NULL,
    customer_zip_code_prefix   INT NULL,
    customer_city              VARCHAR(255) NULL,
    customer_state             VARCHAR(50) NULL
);
GO
-------------------------------
-- 2. olist_geolocation_dataset
-------------------------------
IF OBJECT_ID('silver.olist_geolocation_dataset', 'U') IS NOT NULL
    DROP TABLE silver.olist_geolocation_dataset;
GO

CREATE TABLE silver.olist_geolocation_dataset (
    geolocation_zip_code_prefix INT NOT NULL,
    geolocation_lat             FLOAT NULL,
    geolocation_lng             FLOAT NULL,
    geolocation_city            VARCHAR(255) NULL,
    geolocation_state           VARCHAR(10) NULL
);
GO

-------------------------------
-- 3. olist_order_items_dataset
-------------------------------
IF OBJECT_ID('silver.olist_order_items_dataset', 'U') IS NOT NULL
    DROP TABLE silver.olist_order_items_dataset;
GO

CREATE TABLE silver.olist_order_items_dataset (
    order_id             VARCHAR(50) NULL,
    order_item_id        INT NULL,
    product_id           VARCHAR(50) NULL,
    seller_id            VARCHAR(50) NULL,
    shipping_limit_date  DATETIME NULL,
    price                DECIMAL(10,2) NULL,
    freight_value        DECIMAL(10,2) NULL
);
GO

-------------------------------
-- 4. olist_order_payments_dataset
-------------------------------
IF OBJECT_ID('silver.olist_order_payments_dataset', 'U') IS NOT NULL
    DROP TABLE silver.olist_order_payments_dataset;
GO

CREATE TABLE silver.olist_order_payments_dataset (
    order_id              VARCHAR(50) NULL,
    payment_sequential    INT NULL,
    payment_type          VARCHAR(50) NULL,
    payment_installments  INT NULL,
    payment_value         DECIMAL(10,2) NULL
);
GO

-------------------------------
-- 5. olist_order_reviews_dataset
-------------------------------
IF OBJECT_ID('silver.olist_order_reviews_dataset', 'U') IS NOT NULL
    DROP TABLE silver.olist_order_reviews_dataset;
GO

CREATE TABLE silver.olist_order_reviews_dataset (
    review_id               VARCHAR(50) NULL,
    order_id                VARCHAR(50) NULL,
    review_score            INT NULL,
    review_comment_title    VARCHAR(255) NULL,
    review_comment_message  VARCHAR(MAX) NULL,
    review_creation_date    DATETIME NULL,
    review_answer_timestamp DATETIME NULL
);
GO

-------------------------------
-- 6. olist_orders_dataset
-------------------------------
IF OBJECT_ID('silver.olist_orders_dataset', 'U') IS NOT NULL
    DROP TABLE silver.olist_orders_dataset;
GO

CREATE TABLE silver.olist_orders_dataset (
    order_id                        VARCHAR(50) NULL,
    customer_id                     VARCHAR(50) NULL,
    order_status                    VARCHAR(50) NULL,
    order_purchase_timestamp        DATETIME NULL,
    order_approved_at               DATETIME NULL,
    order_delivered_carrier_date    DATETIME NULL,
    order_delivered_customer_date   DATETIME NULL,
    order_estimated_delivery_date   DATETIME NULL
);
GO

-------------------------------
-- 7. olist_products_dataset
-------------------------------
IF OBJECT_ID('silver.olist_products_dataset', 'U') IS NOT NULL
    DROP TABLE silver.olist_products_dataset;
GO

CREATE TABLE silver.olist_products_dataset (
    product_id                 VARCHAR(50) NULL,
    product_category_name      VARCHAR(255) NULL,
    product_name_lenght        INT NULL,
    product_description_lenght INT NULL,
    product_photos_qty         INT NULL,
    product_weight_g           INT NULL,
    product_length_cm          INT NULL,
    product_height_cm          INT NULL,
    product_width_cm           INT NULL
);
GO

-------------------------------
-- 8. olist_sellers_dataset
-------------------------------
IF OBJECT_ID('silver.olist_sellers_dataset', 'U') IS NOT NULL
    DROP TABLE silver.olist_sellers_dataset;
GO

CREATE TABLE silver.olist_sellers_dataset (
    seller_id              VARCHAR(50) NULL,
    seller_zip_code_prefix INT NULL,
    seller_city            VARCHAR(255) NULL,
    seller_state           VARCHAR(5) NULL
);
GO

-------------------------------
-- 9. product_category_name_translation
-------------------------------
IF OBJECT_ID('silver.olist_product_category_name_translation', 'U') IS NOT NULL
    DROP TABLE silver.olist_product_category_name_translation;
GO

CREATE TABLE silver.olist_product_category_name_translation (
    product_category_name         VARCHAR(255) NULL,
    product_category_name_english VARCHAR(255) NULL
);
GO

IF OBJECT_ID('silver.dim_date', 'U') IS NOT NULL
    DROP TABLE silver.dim_date;
GO

CREATE TABLE silver.dim_date (
    date_key               INT          NOT NULL PRIMARY KEY,
    full_date              DATE         NOT NULL,

    -- Calendar
    year                   SMALLINT     NOT NULL,
    year_text              CHAR(4)      NOT NULL,
    quarter_number         TINYINT      NOT NULL,
    quarter                CHAR(2)      NOT NULL,
    month_number           TINYINT      NOT NULL,
    month_text             CHAR(2)      NOT NULL,
    month_name_full        VARCHAR(20)  NOT NULL,
    month_name_short       CHAR(3)      NOT NULL,
    week_number_iso        TINYINT      NOT NULL,
    week_text              CHAR(4)      NOT NULL,
    day_of_month           TINYINT      NOT NULL,
    day_of_year            SMALLINT     NOT NULL,
    day_name_full          VARCHAR(20)  NOT NULL,
    day_name_short         CHAR(3)      NOT NULL,

    -- Weekend flags
    is_weekend             BIT          NOT NULL,
    is_weekday             BIT          NOT NULL,

    -- Dynamic holiday flags
    is_brazilian_holiday   BIT          NOT NULL,
    holiday_name           VARCHAR(50)  NULL,

    -- Special retail / e-commerce dates
    is_black_friday        BIT          NOT NULL,
    is_mothers_day         BIT          NOT NULL,
    is_valentines_day      BIT          NOT NULL,
    is_childrens_day       BIT          NOT NULL,
    is_consumers_day       BIT          NOT NULL,

    -- Fiscal calendar (starts July 1)
    fiscal_year            SMALLINT     NOT NULL,
    fiscal_quarter         CHAR(3)      NOT NULL
);
GO

CREATE UNIQUE INDEX IX_dim_date_full_date ON silver.dim_date(full_date);
GO
