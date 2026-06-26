## 🚗 Ridesharing Data Engineering Pipeline ##
A complete end-to-end data engineering pipeline built using Medallion Architecture (Bronze → Silver → Validation → Gold) on Databricks, orchestrated with Apache Airflow, and visualized with Power BI.

## 🏗️ Architecture ##
Raw Data (CSV) → Bronze Layer → Silver Layer → Validation → Gold Layer → Power BI Dashboard

## 🛠️ Tech Stack ##

Databricks → Data processing and storage
PySpark → Big data processing
SQL → Data querying
NumPy & Pandas → Data manipulation
Apache Airflow → Pipeline orchestration
Power BI → Data visualization
Delta Lake → Data storage format
GitHub → Version control


## 📁 Project Structure ##
rides_pipeline.py → Airflow DAG 
notebooks/Bronze_layer.py → Raw data ingestion 
notebooks/Silver_layer.py → Data cleaning
notebooks/Validation.py → Data validation
notebooks/Gold_layer.py → Aggregations
screenshots/ → Power BI dashboard screenshots
Ridesharing_dashboard.pbix → Power BI dashboard file

## 🥉 Bronze Layer ##

Ingests raw CSV data into Databricks
Saves as Delta table bronze_ridesharing

## 🥈 Silver Layer ##

Drops duplicates
Handles null values
Fixes data types
Filters invalid data
Standardizes text columns

## ✅ Validation Layer ##

Row count check
Duplicate check
Null check on critical columns
Data type validation
Rating range validation
Positive values check

## 🥇 Gold Layer ##

Revenue per driver
Rides per rider
Average driver rating
Revenue by vehicle type
Revenue by payment type
Peak hours analysis
Cancellation analysis
Top pickup/dropoff locations
OS platform analysis
Surge zone analysis


## 📊 Power BI Dashboard ## 
4 pages:

Admin Dashboard → KPI cards and overview
Revenue Analysis → Revenue breakdowns
Ride Analysis → Trip patterns and locations
Cancellation Analysis → Cancellation insights


## 👤 Author ##
Soumya Jaiswal

GitHub: SoumyaSamriddhiJ
Email: SoumyaJaiswal261@gmail.com
