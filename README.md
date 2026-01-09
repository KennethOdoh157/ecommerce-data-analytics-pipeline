# 📊 Olist E-Commerce Data Analytics Pipeline

**End-to-End SQL Data Warehouse & Power BI Analytics Project**

---

## 📑 Table of Contents

* [Project Overview](#-project-overview)
* [Architecture & Data Pipeline](#-architecture--data-pipeline)

  * [Bronze Layer – Raw Ingestion](#-bronze-layer--raw-ingestion)
  * [Silver Layer – Cleaning & Enrichment](#-silver-layer--cleaning--enrichment)
  * [Gold Layer – Analytics & Modeling](#-gold-layer--analytics--modeling)
* [Data Modeling](#-data-modeling)
* [Analytics & Dashboards (Power BI)](#-analytics--dashboards-power-bi)

  * [Dashboard 1 – Sales & Revenue Overview](#dashboard-1--sales--revenue-overview)
  * [Dashboard 2 – Product & Revenue Performance](#dashboard-2--product--revenue-performance)
  * [Dashboard 3 – Customer & Seller Behavior](#dashboard-3--customer--seller-behavior)
  * [Dashboard 4 – Order & Delivery Performance](#dashboard-4--order--delivery-performance)
* [DAX Measures & Business Logic](#-dax-measures--business-logic)
* [Results & Key Insights](#-results--key-insights)
* [Exploratory & Analytical SQL](#-exploratory--analytical-sql)
* [Business Questions Answered](#-business-questions-answered)
* [Tools & Technologies](#-tools--technologies)
* [Why This Project Stands Out](#-why-this-project-stands-out)
* [Author](#-author)

---

## 🚀 Project Overview

This project is a **production-style data analytics pipeline** built on the Brazilian **Olist e-commerce dataset**, designed to demonstrate how raw transactional data can be transformed into **business-ready insights** through structured data modeling, SQL transformations, and advanced Power BI analytics.

The solution follows a **modern analytics architecture**:

* Layered **Bronze → Silver → Gold** data warehouse design
* Star-schema modeling for analytical performance
* Robust **DAX measures** for business KPIs
* Executive-ready **Power BI dashboards** for decision-making

This project mirrors how analytics is implemented in real organizations — from ingestion to insight.

---

## 🧱 Architecture & Data Pipeline

### 🔹 Bronze Layer – Raw Ingestion

**Path:** `sql/bronze/`

* Raw CSV files ingested directly into SQL Server
* No transformations applied
* Preserves data lineage, traceability, and auditability

📌 *Purpose:* Act as the immutable source of truth.

---

### 🔹 Silver Layer – Cleaning & Enrichment

**Paths:**

* `sql/silver/ddl_silver_layer.sql`
* `sql/silver/proc_silver_layer.sql`

Key transformations:

* Data type standardization
* Null handling and deduplication
* Timestamp normalization
* Geographic enrichment (customers & sellers)
* Referential integrity checks

📌 *Purpose:* Produce clean, reliable, analysis-ready data.

---

### 🔹 Gold Layer – Analytics & Modeling

**Path:** `sql/gold/ddl_gold_layer.sql`

* Business-focused fact and dimension tables
* Optimized for Power BI consumption
* Supports multiple fact tables without fact-to-fact relationships
* Enables flexible time intelligence and cross-fact analysis

📌 *Purpose:* Deliver trusted, high-performance analytical models.

---

## 🗂️ Data Modeling

The project uses a **star schema design**, centered around multiple fact tables:

* `fact_orders`
* `fact_order_items`
* `fact_payments`
* Supporting dimensions (customers, sellers, products, date, geography)

Each fact table is modeled independently with shared dimensions, enabling:

* Accurate aggregation
* Clear filter propagation
* Scalable analytics design

📄 Diagram files:

* `docs/star_schema.png`
* `docs/data_architecture.png`
* `docs/data_flow_diagram.png`
* `docs/data_integration_model.png`

---

## 📈 Analytics & Dashboards (Power BI)

### Dashboard 1 – Sales & Revenue Overview

**File:** `powerbi/dashboards/01-sales-and-revenue-overview.png`

Focus:

* Total Revenue, Orders, Customers, AOV
* Revenue & order trends over time
* Top product categories by revenue
* Payment method analysis

Audience: **Executives & commercial stakeholders**

---

### Dashboard 2 – Product & Revenue Performance

**File:** `powerbi/dashboards/02-product-and-revenue-performance.png`

Focus:

* Product and category performance
* Revenue and item contribution analysis
* Freight cost efficiency
* Dynamic Top-N category analysis

Audience: **Merchandising & product teams**

---

### Dashboard 3 – Customer & Seller Behavior

**File:** `powerbi/dashboards/03-olist-customer-and-seller-behavior.png`

Focus:

* One-time vs repeat customers
* Order frequency segmentation
* Revenue per customer
* Seller activity and participation

Audience: **Growth, retention & marketplace teams**

---

### Dashboard 4 – Order & Delivery Performance

**File:** `powerbi/dashboards/04-olist-order-and-delivery-performance.png`

Focus:

* Delivery timeliness
* Fulfillment and cancellation rates
* Shipping cost efficiency
* Operational performance KPIs

Audience: **Operations & logistics teams**

---

## 🧮 DAX Measures & Business Logic

**Path:** `powerbi/dax_measures/`

Measures are modular, reusable, and grouped by analytical domain:

* Core Metrics
* Sales & Revenue
* Product Performance
* Customer & Seller Behavior
* Order & Delivery Performance
* Time Intelligence

Advanced techniques used:

* `TREATAS` for cross-fact filtering
* Dynamic Top-N logic
* Context-aware measures for multi-dashboard reuse

---

## 📊 Results & Key Insights

This project delivers **actionable business insights**, not just visuals.

### 📈 Revenue & Sales

* Revenue growth is **driven by a small subset of product categories**, confirming a classic Pareto (80/20) distribution.
* Month-over-month analysis highlights **seasonal demand patterns**, useful for inventory and campaign planning.
* Average Order Value (AOV) varies significantly by category, indicating opportunities for **bundling and upsell strategies**.

### 📦 Product & Freight Performance

* Freight costs materially impact profitability for **low-priced, high-volume products**.
* Categories with **higher weight and volume** generate lower revenue per kg, signaling potential pricing or logistics optimization opportunities.
* Dynamic Top-N analysis helps isolate **true revenue drivers** without noise.

### 👥 Customer Behavior

* A **large majority of customers are one-time buyers**, highlighting retention as a key growth lever.
* Repeat customers generate **disproportionately higher revenue per customer**, justifying targeted loyalty strategies.
* Order frequency segmentation enables more precise customer targeting.

### 🚚 Operations & Delivery

* While most orders are delivered successfully, a **non-trivial portion arrive late**, impacting customer experience.
* On-time delivery rate varies by region, pointing to **logistics bottlenecks**.
* Shipping cost per order provides a clear KPI for **fulfillment efficiency monitoring**.

📌 *Overall insight:*
The analysis reveals that **growth, profitability, and customer experience are tightly linked**, and improvements require coordinated action across product, logistics, and retention strategies.

---

## 🔍 Exploratory & Analytical SQL

**Paths:**

* `sql/01_EDA`
* `sql/02_analytics`

Includes:

* Data quality validation
* Distribution and outlier analysis
* Business question exploration
* Metric sanity checks before BI modeling

---

## 🧠 Business Questions Answered

Documented in:

* `docs/business_questions.md`

Examples:

* What drives revenue growth?
* Which products and categories contribute most?
* How efficient is order fulfillment?
* What proportion of customers are repeat buyers?
* Where are logistics bottlenecks occurring?

---

## 🛠️ Tools & Technologies

* **SQL Server** – Data warehousing & transformations
* **Power BI** – Visualization & analytics
* **DAX** – Advanced business metrics
* **Star Schema Modeling**
* **Git & GitHub** – Version control & documentation

---

## 🌟 Why This Project Stands Out

✅ End-to-end ownership (raw data → insights)
✅ Production-style data warehouse design
✅ Advanced DAX and cross-fact analytics
✅ Business-driven dashboards, not vanity metrics
✅ Clear documentation and professional structure

This project reflects how **data analysts and analytics engineers operate in real-world environments**.

---

## 📌 Author

**Odoh Kenneth**
Data Analyst | Analytics Engineer
Focused on building **scalable, insight-driven data solutions**

---


