# 🍺 Brewery Analytics Pipeline

## 📌 Project Overview
This project demonstrates a complete **ELT (Extract, Load, Transform)** pipeline designed to analyze brewery data across the United States. 

The goal was to build a scalable architecture that ingests unstructured data from an API, processes it through a **Medallion Architecture (Bronze, Silver, Gold)**, and delivers a Star Schema ready for BI tools.

## 🛠️ Tech Stack
* **Ingestion:** Python (`Requests`, `Pandas`)
* **Data Warehouse & Transformation:** DuckDB (SQL)
* **Modeling:** Dimensional Modeling (Star Schema)
* **Visualization:** Power BI
* **Orchestration:** Python (Simulating Airflow DAG structure)

## 🏗️ Architecture

The pipeline follows a strict separation of concerns:

1.  **Extract:** Python script hits the Open Brewery DB API (paginated).
2.  **Load (Bronze):** Raw JSON data is converted to Parquet for efficient storage.
3.  **Transform (Silver):** DuckDB cleanses data (deduplication, null handling, type casting).
4.  **Model (Gold):** Creation of Fact and Dimension tables:
    * `fact_breweries`: Central table with keys and metrics.
    * `dim_location`: Normalized geography data.
    * `dim_brewery_type`: Categorical data for filtering.

## 🚀 How to Run

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/YOUR-USERNAME/brewery-analytics-pipeline.git](https://github.com/YOUR-USERNAME/brewery-analytics-pipeline.git)
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Run the pipeline:**
    ```bash
    python main.py
    ```
    *Output: Processed CSV files will appear in the `data/gold` folder.*

## 📊 Analytics & Insights (Power BI)
The final data model allows for answering business questions such as:
* Which states have the highest density of Micro Breweries?
* What is the geographical distribution of breweries (Map Visualization)?
* Comparison of brewery types by region.

---
*Developed by Wesley Bomfim - [LinkedIn Profile](https://www.linkedin.com/in/wesley-bomfim-181401298)*
