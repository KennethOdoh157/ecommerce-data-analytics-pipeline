# Project Overview

## Background

This project is an end-to-end **e-commerce data analytics pipeline** built using the Olist Brazilian e-commerce dataset. The goal is to demonstrate how raw transactional data can be transformed into reliable, analytics-ready datasets and visualized through interactive Power BI dashboards to support business decision-making.

The project follows a modern analytics engineering approach, combining data warehousing concepts, dimensional modeling, and business intelligence reporting.

---

## Business Problem

E-commerce platforms generate large volumes of data across orders, customers, sellers, products, payments, and reviews. However, raw data alone is not suitable for analysis due to quality issues, complex relationships, and lack of structure.

Key challenges addressed in this project include:

* Fragmented transactional data across multiple tables
* Difficulty analyzing customer behavior and repeat purchases
* Limited visibility into geographic and payment trends
* Performance issues when querying raw operational data

---

## Project Objectives

The main objectives of this project are to:

* Design a scalable **Bronze–Silver–Gold** data architecture
* Clean and standardize raw e-commerce data using SQL
* Build a **dimensional star schema** with multiple fact tables
* Enable cross-domain analysis (orders, payments, customers, geography)
* Deliver interactive Power BI dashboards for business insights

---

## Dataset Description

The project uses the **Olist Brazilian E-commerce Dataset**, which contains real-world transactional data including:

* Customers and sellers with geographic information
* Orders and order items
* Payment methods and installments
* Product categories
* Customer reviews and ratings

The dataset reflects real operational challenges such as missing values, orphan records, and varying transaction granularity.

---

## Data Architecture

The pipeline is structured into three logical layers:

### Bronze Layer (Raw / Staging)

* Stores raw ingested data with minimal transformation
* Handles type casting, basic cleaning, and standardization
* Preserves source-level granularity

### Silver Layer (Business-Ready)

* Implements cleaned and conformed **dimension tables**
* Builds multiple **fact tables** at different grains (orders, items, payments, reviews)
* Applies surrogate keys and enforces analytical consistency

### Gold Layer (Analytics / Presentation)

* Creates analytical marts optimized for reporting
* Aggregates data for performance and usability
* Serves as the primary data source for Power BI dashboards

---

## Data Modeling Approach

A **star schema** design was adopted to support efficient analytical queries and intuitive reporting.

Key modeling decisions include:

* Separate fact tables for orders, order items, payments, and reviews
* Shared dimensions (customers, sellers, products, geography, date)
* A reusable date dimension connected to multiple fact tables
* Avoidance of fact-to-fact relationships in the semantic layer

This approach enables flexible analysis while maintaining model simplicity and performance.

---

## Analytics & Reporting

The Gold layer feeds a Power BI data model used to create multiple dashboards, including:

* Executive overview of revenue and orders
* Sales performance by product and time
* Customer behavior and repeat purchase analysis
* Payment methods and installment trends
* Geographic insights by city and state

The dashboards are designed with usability, consistency, and business relevance in mind.

---

## Assumptions & Limitations

* The dataset represents a historical snapshot and is not real-time
* Currency values are analyzed in the dataset’s original currency
* Some records contain missing or incomplete information
* The project focuses on analytical use cases rather than operational reporting

---

## Tools & Technologies

* **SQL** – data cleaning, transformation, and modeling
* **Relational Database** – data warehousing and analytics storage
* **Power BI** – data modeling and visualization
* **GitHub** – version control and project documentation

---

## Outcome

This project demonstrates the complete lifecycle of an analytics solution, from raw data ingestion to business-ready insights, highlighting practical skills in data modeling, SQL analytics engineering, and business intelligence reporting.
