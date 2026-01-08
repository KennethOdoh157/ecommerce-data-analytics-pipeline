
# 📚 **Data Dictionary for Gold Layer Views**

## 📊 **Gold Layer Overview**

The **Gold Layer** represents the **final, analytics-ready semantic layer** of the data warehouse.
It is modeled using a **star schema** and designed for **BI tools (Power BI), SQL analytics, and advanced reporting**.

This layer:

* Consolidates cleaned data from the Silver layer
* Applies **business logic, surrogate keys, and conformed dimensions**
* Supports **time intelligence, customer analysis, product performance, and logistics insights**

---

## 📅 **View: `gold.dim_date`**

This view provides a **comprehensive calendar dimension** used for time-based analysis, seasonality, and holiday insights.

| **Column Name**        | **Data Type** | **Description**                                   |
| ---------------------- | ------------- | ------------------------------------------------- |
| `date_key`             | INT           | Surrogate key representing a unique calendar date |
| `full_date`            | DATE          | Calendar date (YYYY-MM-DD)                        |
| `year`                 | INT           | Calendar year                                     |
| `year_text`            | NVARCHAR      | Calendar year (text format)                       |
| `quarter_number`       | INT           | Quarter number (1–4)                              |
| `quarter`              | NVARCHAR      | Quarter label (e.g., Q1)                          |
| `month_number`         | INT           | Month number (1–12)                               |
| `month_text`           | NVARCHAR      | Month number (text)                               |
| `month_name_full`      | NVARCHAR      | Full month name                                   |
| `month_name_short`     | NVARCHAR      | Abbreviated month name                            |
| `week_number_iso`      | INT           | ISO week number                                   |
| `week_text`            | NVARCHAR      | ISO week label                                    |
| `day_of_month`         | INT           | Day number within the month                       |
| `day_of_year`          | INT           | Day number within the year                        |
| `day_name_full`        | NVARCHAR      | Full weekday name                                 |
| `day_name_short`       | NVARCHAR      | Abbreviated weekday name                          |
| `is_weekend`           | BIT           | Indicates weekend (1 = weekend)                   |
| `is_weekday`           | BIT           | Indicates weekday (1 = weekday)                   |
| `is_brazilian_holiday` | BIT           | Brazilian public holiday flag                     |
| `holiday_name`         | NVARCHAR      | Holiday name                                      |
| `is_black_friday`      | BIT           | Black Friday indicator                            |
| `is_mothers_day`       | BIT           | Mother’s Day indicator                            |
| `is_valentines_day`    | BIT           | Valentine’s Day indicator                         |
| `is_childrens_day`     | BIT           | Children’s Day indicator                          |
| `is_consumers_day`     | BIT           | Consumer’s Day indicator                          |
| `fiscal_year`          | INT           | Fiscal year                                       |
| `fiscal_quarter`       | NVARCHAR      | Fiscal quarter                                    |

---

## 👤 **View: `gold.dim_customer`**

This view stores **customer master data** enriched with geographic coordinates.

| **Column Name**            | **Data Type** | **Description**                           |
| -------------------------- | ------------- | ----------------------------------------- |
| `customer_key`             | INT           | Surrogate key for customer                |
| `customer_unique_id`       | NVARCHAR      | Unique business-level customer identifier |
| `customer_id`              | NVARCHAR      | Platform-specific customer ID             |
| `customer_zip_code_prefix` | NVARCHAR      | ZIP code prefix                           |
| `customer_city`            | NVARCHAR      | Customer city                             |
| `customer_state`           | NVARCHAR      | Customer state                            |
| `geolocation_lat`          | FLOAT         | Latitude                                  |
| `geolocation_lng`          | FLOAT         | Longitude                                 |

---

## 🏪 **View: `gold.dim_seller`**

This view represents **seller profiles and locations**, enabling seller-level and regional analysis.

| **Column Name**          | **Data Type** | **Description**            |
| ------------------------ | ------------- | -------------------------- |
| `seller_key`             | INT           | Surrogate seller key       |
| `seller_id`              | NVARCHAR      | Seller business identifier |
| `seller_zip_code_prefix` | NVARCHAR      | ZIP code prefix            |
| `seller_city`            | NVARCHAR      | Seller city                |
| `seller_state`           | NVARCHAR      | Seller state               |
| `geolocation_lat`        | FLOAT         | Latitude                   |
| `geolocation_lng`        | FLOAT         | Longitude                  |

---

## 📦 **View: `gold.dim_product`**

This view contains **product attributes and physical characteristics** for product-level performance and logistics analysis.

| **Column Name**              | **Data Type** | **Description**                     |
| ---------------------------- | ------------- | ----------------------------------- |
| `product_key`                | INT           | Surrogate product key               |
| `product_id`                 | NVARCHAR      | Business product identifier         |
| `product_category_name`      | NVARCHAR      | Original product category           |
| `product_category_name_en`   | NVARCHAR      | English-translated product category |
| `product_name_lenght`        | INT           | Product name length                 |
| `product_description_lenght` | INT           | Product description length          |
| `product_photos_qty`         | INT           | Number of product photos            |
| `product_weight_g`           | FLOAT         | Product weight (grams)              |
| `product_length_cm`          | FLOAT         | Product length (cm)                 |
| `product_height_cm`          | FLOAT         | Product height (cm)                 |
| `product_width_cm`           | FLOAT         | Product width (cm)                  |

---

## 🌍 **View: `gold.dim_geography`**

This view provides a **geographic reference table** based on ZIP code prefixes.

| **Column Name**               | **Data Type** | **Description**         |
| ----------------------------- | ------------- | ----------------------- |
| `geography_key`               | INT           | Surrogate geography key |
| `geolocation_zip_code_prefix` | NVARCHAR      | ZIP code prefix         |
| `geolocation_city`            | NVARCHAR      | City                    |
| `geolocation_state`           | NVARCHAR      | State                   |
| `geolocation_lat`             | FLOAT         | Latitude                |
| `geolocation_lng`             | FLOAT         | Longitude               |

---

## 🧾 **View: `gold.fact_orders`**

This fact table captures **order-level transactional data**.

**Grain:** One row per order

| **Column Name**               | **Data Type** | **Description**                            |
| ----------------------------- | ------------- | ------------------------------------------ |
| `order_id`                    | NVARCHAR      | Unique order identifier                    |
| `customer_key`                | INT           | Foreign key to `dim_customer`              |
| `purchase_date_key`           | INT           | Purchase date (FK to `dim_date`)           |
| `approved_date_key`           | INT           | Approval date (FK to `dim_date`)           |
| `delivered_customer_date_key` | INT           | Delivery date (FK to `dim_date`)           |
| `estimated_delivery_date_key` | INT           | Estimated delivery date (FK to `dim_date`) |
| `order_status`                | NVARCHAR      | Order status                               |
| `delivery_days`               | INT           | Days between purchase and delivery         |
| `order_count`                 | INT           | Order counter (always 1)                   |

---

## 📦 **View: `gold.fact_order_items`**

This fact table stores **item-level order details**, supporting product and seller analysis.

**Grain:** One row per order item

| **Column Name**           | **Data Type** | **Description**                        |
| ------------------------- | ------------- | -------------------------------------- |
| `order_id`                | NVARCHAR      | Order identifier                       |
| `order_item_id`           | INT           | Line item sequence number              |
| `product_key`             | INT           | Foreign key to `dim_product`           |
| `seller_key`              | INT           | Foreign key to `dim_seller`            |
| `shipping_limit_date_key` | INT           | Shipping limit date (FK to `dim_date`) |
| `price`                   | FLOAT         | Item price                             |
| `freight_value`           | FLOAT         | Shipping cost                          |
| `item_count`              | INT           | Item counter (always 1)                |

---

## 💳 **View: `gold.fact_payments`**

This fact table captures **payment transactions**, reflecting realized revenue.

**Grain:** One row per payment record

| **Column Name**        | **Data Type** | **Description**                 |
| ---------------------- | ------------- | ------------------------------- |
| `order_id`             | NVARCHAR      | Order identifier                |
| `payment_sequential`   | INT           | Payment sequence number         |
| `customer_key`         | INT           | Foreign key to `dim_customer`   |
| `payment_date_key`     | INT           | Payment date (FK to `dim_date`) |
| `payment_type`         | NVARCHAR      | Payment method                  |
| `payment_installments` | INT           | Number of installments          |
| `payment_value`        | FLOAT         | Payment amount                  |
| `payment_count`        | INT           | Payment counter (always 1)      |

---

## ⭐ **View: `gold.fact_reviews`**

This fact table stores **customer review and rating data**.

**Grain:** One row per review

| **Column Name**            | **Data Type** | **Description**                         |
| -------------------------- | ------------- | --------------------------------------- |
| `review_id`                | NVARCHAR      | Review identifier                       |
| `order_id`                 | NVARCHAR      | Order identifier                        |
| `customer_key`             | INT           | Foreign key to `dim_customer`           |
| `review_creation_date_key` | INT           | Review creation date (FK to `dim_date`) |
| `review_score`             | INT           | Review rating score (1–5)               |
| `review_count`             | INT           | Review counter (always 1)               |

---

## 🔗 **Modeling Notes**

* Star schema optimized for BI performance
* Surrogate keys used for stability and scalability
* Conformed dimensions shared across fact tables
* Designed for **Power BI semantic modeling and DAX measures**

---

