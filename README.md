Data Pipeline & ETL Processing System

This project is a small end-to-end data engineering pipeline built in Databricks using PySpark and SQL.
The goal was to simulate a real-world scenario where data comes from different sources, needs to be cleaned, validated, and then used for analytics.

Overview

The pipeline follows a layered architecture:

Bronze → raw data ingestion
Silver → cleaning and validation
Gold → business-level analytics

Data comes from CSV and JSON files (customers, products, orders, payments) stored in Databricks Volumes.

Tech Stack
PySpark (DataFrames)
Databricks (notebooks, tables, volumes)
SQL (analytics layer)

Project Structure
01_bronze_ingestion.py
02_silver_cleaning.py
03_validation_checks.py
04_gold_transformations.py
05_sql_analytics.sql
tests.yml

Bronze Layer

In this layer, I read raw files and store them as tables without modifying the data.

I also added some metadata:

ingestion timestamp
source file name
batch id
record id

The idea is to keep the original data unchanged so it can always be traced back.

Silver Layer

This is where most of the logic is.

I:

cleaned text fields (trim, lowercase where needed)
converted types (dates, numbers)
applied validation rules (null checks, value checks, etc.)

Instead of dropping bad data, I moved it to a quarantine table with information about:

where it came from
why it failed

I also handled duplicates and basic data consistency issues.

Validation Checks

After cleaning, I added cross-table checks like:

orders without customers
orders without products
payments without orders

This helps make sure relationships between tables are valid.

Gold Layer

Here I built business-ready tables.

Main table:

gold_order_facts → combines orders, customers, products, payments

Also created:

gold_customer_summary
gold_product_sales_summary

Some metrics:

order total
payment status
revenue
monthly trends
Data Quality Tests

I added simple data tests in PySpark (and also a YAML example like dbt):

not null checks
uniqueness checks
valid value checks

Results are stored in a data_quality_results table.

Example Analytics

Using SQL on top of Gold tables:

top products by revenue
top customers
monthly revenue trends
payment success rate
cancelled orders
What I Learned
how to structure an ETL pipeline (Bronze → Silver → Gold)
how to work with PySpark DataFrames
importance of data quality and validation
how to handle bad data instead of ignoring it
how to design data for analytics use cases
Notes

This is a learning project, but I tried to keep it close to how real pipelines are structured.
It can be extended further with streaming, orchestration, or external databases.