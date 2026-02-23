# Project Objective

This project implements a simplified Retail Analytics System that processes retail transaction data and generates business insights.

The system:

Ingests and cleans raw sales data

Stores validated data in a relational database

Calculates customer loyalty points

Performs customer segmentation (RFM-based)

Applies basic predictive analytics

Generates analytical dashboards

The architecture is modular, scalable, and designed for clarity and maintainability.


# Project Architecture => High-Level Architecture:

Raw Data ➜ Clean Data ➜ Structured Database ➜ Business Logic ➜ Insights Dashboard


🟦 STEP 1: RAW DATA
   📁 data/raw/
   • Stores.csv
   • Products.csv
   • Customers.csv
   • Sales.csv

        │
        ▼

🟩 STEP 2: ETL PIPELINE (Data Processing Engine)
   📥 ingest.py      → Read CSV files
   🧹 validate.py    → Clean & validate
   📤 load.py        → Insert into database

        │
        ▼

🟨 STEP 3: DATABASE (Central Brain)
   💾 database/retail.db
   • Master Tables
   • Transaction Tables
   • Rules Tables

        │
        ▼

🟧 STEP 4: ANALYTICS ENGINE (Business Intelligence)
   🎯 loyalty.py
   👥 segmentation.py
   📊 predictive.py

        │
        ▼

🟪 STEP 5: DASHBOARD (Insights Layer)
   📈 dashboard.py
   • Sales Trends
   • Top Products
   • Loyalty Distribution


# Database Design

We use SQLite (retail.db) as a relational database.

Core Tables

stores – Store information

products – Product catalog & stock

customer_details – Customer & loyalty data

store_sales_header – Transaction summary

store_sales_line_items – Product-level sales

promotion_details – Promotion rules

loyalty_rules – Loyalty calculation logic

# Design Approach:

Normalized relational schema

Primary & foreign key relationships

Separation of transactional and master data

Designed to support analytics queries efficiently

# ETL Strategy

The ETL pipeline is modular and divided into three steps:

# Ingestion

Reads CSV files from data/raw

Converts into structured dataframes

# Validation

Performs:

Null checks

Negative value checks

Special character cleaning

Datatype validation

Basic integrity checks

Invalid records are handled separately.

# Loading

Loads cleaned data into retail.db

Supports reusable and incremental data loads

This ensures high data quality before analytics execution.

# Loyalty & Segmentation
# Loyalty Logic

Points assigned based on spend rules

Bonus points applied using thresholds

Stored in customer_details.total_loyalty_points

RFM Segmentation

Recency (days since last purchase)

Frequency (number of transactions)

Monetary (total spend)

Segments:

HS – High Spender

AR – At Risk

# Predictive Analytics

Includes simple business-rule-based predictions:

Next month spend estimation

Stock-out risk detection

Promotion response sensitivity

No complex ML — logic-driven as per scope.

# Dashboard

Built using Matplotlib.

Includes:

Total sales by store

Top selling products

Loyalty distribution