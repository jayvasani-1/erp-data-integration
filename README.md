ERP Data Integration Pipeline

End-to-end ERP data integration system built in Python — parses industrial EDIFACT order files, loads structured data into SQL Server through a dimensional data model, and connects directly to Power BI and Tableau for business intelligence reporting. The entire pipeline runs with a single command.

## What Was Built and Why

### 1. EDIFACT Message Parser
- Parses raw EDIFACT EDI order files (ORDERS segment standard) from the data/sample_edifact/ directory
- Extracts structured order, customer, and product data into staging CSV files
- Purpose: Replace manual data entry from supplier EDI messages into the ERP system

### 2. SQL Server Database Layer
- Automated schema creation, including fact and dimension tables (Star Schema)
- Stored procedures for upsert operations: usp_upsert_customer, usp_upsert_product, usp_load_orders, usp_stage_orders_from_flat
- BI-ready views created automatically via bi_views.sql
- Purpose: Establish a single source of truth for order and financial data

### 3. Python ETL Pipeline
- test_connection.py — validates SQL Server connectivity via ODBC before pipeline runs
- setup_db.py — deploys schema, stored procedures, and BI views to SQL Server
- parse_edifact.py — parses EDIFACT files into structured staging CSVs
- load_sqlserver.py — loads staging CSVs into SQL Server using stored procedures
- Purpose: Fully automated data flow from raw EDI messages to analytics-ready database

### 4. BI Integration Layer
- ERP_DEMO.pbids — Power BI connection file pointing to ERP_DEMO database
- tableau_connection.tds — Tableau data source connecting to SQL Server via integrated auth
- Auto-opens BI tools on pipeline completion (Windows)
- Purpose: Connect business intelligence tools directly to the loaded ERP data

### 5. One-Command Deployment
- run_project.py installs dependencies, validates DB connection, deploys schema, parses EDIFACT, loads SQL Server, and opens BI connections in sequence
- .env.example provided for environment-based configuration — no hardcoded credentials
- Purpose: Reproducible, portable deployment for any SQL Server environment

## Tech Stack
Python, SQL Server, ODBC, EDIFACT (EDI), Power BI, Tableau, python-dotenv, tqdm

## How to Run
1. Copy .env.example to .env and fill in your SQL Server credentials
2. Run the full pipeline:
   python run_project.py

## Project Structure
erp-data-integration/
├── run_project.py          # One-command pipeline orchestrator
├── quick_test.py           # Standalone DB connection tester
├── requirements.txt
├── .env.example            # Environment variable template
├── etl/
│   ├── parse_edifact.py    # EDIFACT parser → staging CSVs
│   ├── setup_db.py         # Schema + stored procedure deployment
│   ├── load_sqlserver.py   # Staging CSV → SQL Server loader
│   └── test_connection.py  # DB connectivity check
├── db/sqlserver/
│   ├── schema.sql          # Dimensional data model
│   ├── bi_views.sql        # BI-ready analytical views
│   └── procs/              # Stored procedures (upsert logic)
├── data/sample_edifact/    # Sample EDIFACT order files
└── bi/
    ├── ERP_DEMO.pbids      # Power BI connection file
    └── tableau_connection.tds  # Tableau connection file"
