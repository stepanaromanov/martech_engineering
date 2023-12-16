# Overview
Welcome to the Educational Company ETL (Extract, Transform, Load) Showcase! This repository provides a practical example of how an ETL program can be implemented for an educational company with four key departments: Marketing, Sales, Services, and Finance.

## commands.sh

This guide outlines a quick setup process for installing PostgreSQL database and Apache Superset for Business Intelligence (BI) reporting using Docker containers.

## Features
Departmental Scripts: The ETL process is broken down into four Python scripts, each dedicated to a specific department—Marketing, Sales, Services, and Finance. These scripts are designed to extract relevant data from various sources associated with each department.

## Data Sources and Integration:

### CRM Integration
The Python scripts seamlessly connect to the Customer Relationship Management (CRM) system to fetch essential customer-related data and rooms capacity data.

### Google Sheets Integration
Extracting and processing data from Google Sheets, ensuring that the latest information is incorporated into the analysis.

### Facebook Advertising Integration
Retrieving data from the Facebook Advertising Manager to gain insights into marketing campaigns and audience engagement.

### Google Analytics Integration
Gathering data from Google Analytics to analyze website traffic and user behavior.

### Data Transformation with Pandas and Numpy
The extracted data undergoes a comprehensive data wrangling transformation using powerful Python libraries, including Pandas and Numpy. This ensures that the data is clean, structured, and ready for further analysis.

### PostgreSQL Database Integration
The transformed data is loaded into a PostgreSQL database, providing a centralized repository for easy storage, retrieval, and management of educational company information.

After that data could be processed using SQL Lab within Apache Superset. Decision makers can gain valuable insights through interactive dashboards and reports.