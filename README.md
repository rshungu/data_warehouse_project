# **Data Warehouse and Analytics Project**
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-Active-brightgreen.svg)]()
[![SQL](https://img.shields.io/badge/SQL-PostgreSQL-blue)]()
[![ETL](https://img.shields.io/badge/ETL-Pipelines-yellow)]()

🚀 Welcome to the Data Warehouse and Analytics Project Repository

This project presents a complete data warehousing and analytics solution, covering everything from data ingestion to generating actionable insights. Designed as a portfolio project, it adheres to industry best practices in data engineering and analytics, demonstrating efficient data modeling, transformation, and reporting.

---
## Data Architecture  

The project follows the **Medallion Architecture**, which is organized into three layers:  

![Medallion Architecture](https://github.com/rshungu/data_warehouse_project/blob/33c4f85dbd2cad5bb8be981da2f72e63d7357dc2/scripts/Data_Architecture.jpg)

- **Bronze Layer**: Stores raw data directly from source systems.
- **Silver Layer**: Cleans, standardizes, and organizes the data to make it ready for analysis.  
- **Gold Layer**: Transforms the data into a structured model suitable for reporting and generating insights.  

---
## 📌 Project Overview

This project focuses on building a modern data solution with the following components:

1. **Data Architecture:**  
  Implementing a Medallion Architecture with **Bronze**, **Silver**, and **Gold** layers to ensure scalable and reliable data management.

2. **ETL Pipelines:**  
  Designing and developing ETL processes to **extract**, **transform**, and **load** data from source systems into the data warehouse.

3. **Data Modeling:**  
  Creating optimized **fact** and **dimension** tables to support efficient analytical queries.

4. **Analytics & Reporting:**  
  Developing **SQL-based reports** and **dashboards** to deliver meaningful and actionable insights.

---

## 🛠 Project Requirements

### A. Building the Data Warehouse (Data Engineering)
#### Objective

Design and implement a modern data warehouse using **SQL Server** to consolidate sales data, supporting analytical reporting and informed business decision-making.

#### Specifications

- **Data Sources:**  
  Load data from two source systems — **ERP** and **CRM** — provided as CSV files.

- **Data Quality:**  
  Perform data cleansing to identify and resolve quality issues before analysis.

- **Integration:**  
  Merge both sources into a unified, user-friendly **data model** optimized for analytical queries.

- **Scope:**  
  Focus on the most recent dataset only (no historization or time-based tracking required).

- **Documentation:**  
  Deliver clear and concise documentation of the data model to support both business stakeholders and analytics teams.

### B. BI Analytics and Reporting
#### Objective

Create **SQL-based analytics** to provide comprehensive insights into:

- **Customer Behavior**
- **Product Performance**
- **Sales Trends**

These insights equip stakeholders with critical business metrics to support **strategic decision-making** and drive informed actions.
  
---
## 📂 Repository Structure

```
data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                               # Project documentation and architecture details
│   ├── etl.drawio                      # Draw.io file shows all different techniquies and methods of ETL
│   ├── data_architecture.drawio        # Draw.io file shows the project's architecture
│   ├── data_catalog.md                 # Catalog of datasets, including field descriptions and metadata
│   ├── data_flow.drawio                # Draw.io file for the data flow diagram
│   ├── data_models.drawio              # Draw.io file for data models (star schema)
│   ├── naming-conventions.md           # Consistent naming guidelines for tables, columns, and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instructions
├── LICENSE                             # License information for the repository
├── .gitignore                          # Files and directories to be ignored by Git
└── requirements.txt                    # Dependencies and requirements for the project
```
---
