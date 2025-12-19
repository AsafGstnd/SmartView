# SmartView - Distributed Media Analytics Engine

![Spark](https://img.shields.io/badge/Apache%20Spark-3.0%2B-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.8%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Platform-FF3621?style=for-the-badge&logo=databricks&logoColor=white)

## Summary
This project implements a scalable **Big Data ETL & Analytics Pipeline** designed to process high-volume viewing logs and demographic data for the "Distributed Media Association."

The solution leverages **PySpark** to ingest, clean, and analyze distributed datasets, deriving actionable business insights such as **Household Wealth Scores**, **Program Popularity Indices**, and **Genre Preferences** across different market areas (DMAs). The architecture focuses on performance optimization using Spark SQL best practices to handle millions of records efficiently.

## System Architecture
The pipeline is modularized into two distinct stages to separate data engineering from business intelligence.

### Part 1: Robust ETL Pipeline 
Responsible for ingesting raw data and preparing a clean, type-safe foundation for analysis.
* **Schema Enforcement:** utilization of strict `StructTypes` to validate data quality at ingestion for Reference, Program, and Viewing logs.
* **Data Sanitization:** * Resolution of "clashing records" (conflicting metadata for identical program codes).
    * Deduplication strategies to ensure data integrity.
* **Feature Engineering:** Transformation of categorical socioeconomic indicators (e.g., mapping Income Codes 'A'-'D' to ordinal numeric scales 10-13) to facilitate downstream mathematical modeling.

### Part 2: Advanced Market Analytics 
Focuses on deriving complex metrics using advanced Spark capabilities.
* **Behavioral Segmentation:** Development of a **"Wealth Score" algorithm**, correlating household income data with viewing duration to grade market potential.
* **Window Functions:** Extensive use of `Window.partitionBy()` and `rank()` to calculate granular statistics (e.g., *Top 5 Genres per DMA*) without expensive shuffling.
* **Performance Optimization:** Implementation of **Broadcast Joins** for combining large transaction logs with smaller dimension tables (Demographics/Programs), significantly reducing network overhead.

## 📂 Repository Structure

```text
├── notebooks/
│   ├── SmartView - SparkML Analysis and Metrics.ipynb           # Ingestion, Schema Validation, Cleaning
│   └── SmartView - ETL and Data Cleaning.ipynb    # Wealth Scoring, Aggregations, Reporting
├── data/
│   └── .gitkeep                    # (Data excluded: Hosted on Distributed File System)
├── .gitignore                      # Configured to ignore large CSV/Parquet files
└── README.md                       # Project Documentation
