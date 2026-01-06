/* ============================================================
   PERFORMANCE OPTIMIZATION: MATERIALIZED FACT TABLES & INDEXING
   ============================================================

   CONTEXT
   -------
   Analytical queries built on the Gold
   layer were experiencing slow performance due to repeated
   scans on large fact tables and lack of supporting indexes.

   PURPOSE
   -------
   This script improves query performance by:
   - Materializing frequently accessed Gold fact tables
     into physical tables
   - Creating targeted indexes on high-cardinality and
     commonly filtered/joined columns

   ACTIONS PERFORMED
   -----------------
   1. Drops and recreates:
      - gold.fact_orders_tbl
      - gold.fact_order_items_tbl
   2. Copies data from existing Gold views/tables
   3. Adds non-clustered indexes on:
      - order_id
      - customer_key
      - order_status

   IMPACT
   ------
   - Faster query execution
   - Reduced Power BI refresh time
   - Improved dashboard responsiveness

   NOTE
   ----
   This approach is used intentionally for read-heavy
   analytical workloads where data is refreshed in batches.

   ============================================================ */

-- Materialize Orders Fact Table
IF OBJECT_ID('gold.fact_orders_tbl', 'U') IS NOT NULL
    DROP TABLE gold.fact_orders_tbl;

SELECT *
INTO gold.fact_orders_tbl
FROM gold.fact_orders;

CREATE INDEX idx_fact_orders_order_id
ON gold.fact_orders_tbl (order_id);

CREATE INDEX idx_fact_orders_customer_key
ON gold.fact_orders_tbl (customer_key);

CREATE INDEX idx_fact_orders_status
ON gold.fact_orders_tbl (order_status);


-- Materialize Order Items Fact Table
IF OBJECT_ID('gold.fact_order_items_tbl', 'U') IS NOT NULL
    DROP TABLE gold.fact_order_items_tbl;

SELECT *
INTO gold.fact_order_items_tbl
FROM gold.fact_order_items;

CREATE INDEX idx_fact_items_order_id
ON gold.fact_order_items_tbl (order_id);

