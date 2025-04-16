# Amazon Product Review Analysis

## Metadata

- **Project Name**: Amazon Product Review Analysis  
- **Description**: ETL and analysis pipeline for Amazon customer reviews dataset using Python, MySQL (AWS RDS), and Apache Superset for data visualization.  
- **Dataset Format**: Parquet (converted to CSV for ingestion)  
- **Data Source**: Amazon customer review dataset  
- **Tags**: `ETL`, `Amazon`, `Reviews`, `MySQL`, `Data Analysis`, `Apache Superset`, `Python`

---

## Objectives

- `clean_data`: Clean and preprocess customer review data  
- `load_data`: Load cleaned data into a remote SQL database (AWS RDS - MySQL)  
- `analyze_data`: Perform SQL-based analysis on product reviews  
- `visualize_data`: Use Apache Superset to generate interactive dashboards and reports  

---

## Tech Stack

```yaml
language: Python
database: MySQL (AWS RDS)
etl-library: pandas, mysql-connector-python
visualization: Apache Superset

steps:
  - step: Data Ingestion
    tool: pandas
    description: Load CSV data (from converted Parquet format)

  - step: Data Cleaning
    tasks:
      - remove duplicates
      - handle missing/null values
      - normalize review text
      - format date columns

  - step: Data Modeling & Upload
    description: Create SQL tables and insert data into AWS RDS using mysql-connector

  - step: SQL-Based Analysis
    description: Run SQL queries to extract insights (top products, review trends, ratings)

  - step: Data Visualization
    tool: Apache Superset
    description: Build dashboards from MySQL queries

