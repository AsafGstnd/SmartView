# SmartView
# Distributed Media Association - Data Analysis Project

This project is a big data analysis pipeline built using **PySpark**. It processes large-scale viewing and demographic data for the "Distributed Media Association" to analyze viewer behavior and improve service quality.

## 📌 Project Overview
The goal of this project is to process and analyze distributed data across multiple datasets to derive insights into household viewing habits, program popularity, and demographic trends.

**Key Technologies:** Apache Spark (PySpark), Spark SQL, Databricks.

## 📂 Dataset Description
The analysis relies on four interconnected datasets:
1.  **Reference Data:** Maps device IDs to specific households and Market Areas (DMA).
2.  **Daily Program Data:** Metadata for programs (Program Code, Title, Genre, Air Date/Time, Duration).
3.  **Program Viewing Data:** High-volume log data recording when specific devices viewed specific programs.
4.  **Demographic Data:** Household details including income, size, and other socioeconomic factors.

## 🚀 Notebook Structure

### 1. Data Loading & Cleaning (`notebooks/Part1...`)
* **Schema Definition:** Defines strict StructTypes for efficient data loading.
* **Data Ingestion:** Loads CSV data from the distributed file system (DBFS).
* **Preprocessing:** * Casts encoded columns (e.g., Income codes 'A'-'D') to integers.
    * Handles "clashing" records (e.g., inconsistent genres for the same program code).
    * Removes duplicates and performs validity checks.

### 2. Analysis & Transformation (`notebooks/Part2...`)
* **Advanced Aggregations:** joins viewing logs with demographic and program data.
* **Business Logic:** Calculates metrics such as "Wealth Score" per DMA and analyzes genre preferences.
* **Optimization:** Utilizes Spark Window functions and broadcasting for efficient join operations on large datasets.

## 🛠️ How to Run
1.  **Environment:** This project is designed to run on a Spark cluster (e.g., Databricks or local Spark setup).
2.  **Data Setup:** Ensure the CSV files are mounted to `dbfs:/mnt/coursedata2024/fwm-stb-data/` (or update the file paths in the `load_csv_file` function).
3.  **Execution:** Run the notebooks in order (Part 1 first to validate data, then Part 2).

## 📝 Course Context
This project was created for the **Distributed Database Management (096224)** course at the **Technion - Israel Institute of Technology**.
