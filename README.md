# ✈️ Flights Data Engineering Project — Databricks & Delta Live Tables

## 📘 Overview
This project demonstrates a **modern data engineering pipeline** built on ** Databricks**, designed to ingest, transform, and analyze flight-related datasets using **Delta Lake architecture**. The project follows the **medallion architecture (Landing → Bronze → Silver → Gold)** and leverages **Autoloader** and **Delta Live Tables (DLT)** for incremental data processing and quality enforcement.

---

## 🏗️ Architecture
The project follows a layered data architecture to ensure scalability, data quality, and reliability.


**Layers:**
1. **Landing Layer:** Raw data uploaded directly from GitHub to Databricks Volumes.
2. **Bronze Layer:** Raw ingested data stored in Delta format for incremental and schema-aware ingestion.
3. **Silver Layer:** Cleaned and standardized data with enforced quality rules using Delta Live Tables (DLT).
4. **Gold Layer:** Aggregated and business-ready data prepared for analytics and reporting.

---

## ⚙️ Technologies Used
- **Databricks** – Data processing and transformation.
- **Delta Lake** – Data versioning and ACID transactions.
- **Autoloader** – Incremental file ingestion from Landing to Bronze.
- **Delta Live Tables (DLT)** – Data quality and pipeline orchestration.
- **PySpark** – Transformation and data cleaning.
- **Databricks SQL / Views** – Analytical layer and reporting.
- **GitHub** – Source control for datasets and notebooks.

---


---

## 🧩 Data Flow

### 1️⃣ **Landing Layer**
- The raw flight datasets (from GitHub) are uploaded manually into **Databricks Volumes** under the **Landing** directory.
- Data includes multiple files related to:
  - Flight details
  - Airlines information
  - Bookings and delays

### 2️⃣ **Bronze Layer — Incremental Ingestion**
- Data is loaded incrementally from **Landing** to **Bronze** using **Databricks Autoloader**.
- **Autoloader** enables schema evolution and scalable file discovery.
- Example:
  
  ```python
  df = (spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "csv")
         .option("header", "true")
         .load("/mnt/landing/flights/"))
  
  df.writeStream.format("delta").option("checkpointLocation", checkpoint_path).start(bronze_path).

 ### 3️⃣ Silver Layer — Data Cleaning & Quality

Data from Bronze is cleaned and standardized using Delta Live Tables (DLT).

DLT pipeline defines expectations to enforce data quality:

@dlt.expect("valid_flight_date", "flight_date IS NOT NULL")
@dlt.expect("positive_distance", "distance > 0")


Records failing expectations can be dropped or quarantined.

Transformed data is written to the Silver layer as Delta Tables.

### 4️⃣ Gold Layer — Aggregations & Analytics

Business metrics and KPIs are generated, including:

Top 3 Airlines by number of flights in the current year.

Total number of bookings per airline.

Number of delayed flights per airline.

Data is aggregated and stored in Gold tables for reporting.


## 🧠 Key Features

✅ Incremental Ingestion: Implemented using Autoloader to handle new data efficiently.

✅ Data Quality Enforcement: Used DLT expectations to maintain schema and data integrity.

✅ Modular Design: Each layer is isolated and reusable.

✅ Streaming Capability: Silver layer tables are built as streaming tables for real-time processing.

✅ Scalable Architecture: Handles schema evolution, large data volumes, and multiple sources.





