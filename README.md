# Metro Cash & Carry Data Warehouse and ETL System

## 📖 Overview

This project implements a scalable Data Warehouse (DW) and ETL pipeline for Metro Cash & Carry Pakistan. It integrates transactional and master data to enable strategic reporting and business insights across products, customers, suppliers, stores, and time.

## 🎯 Objectives

- Build a dimensional data warehouse schema in MySQL
- Extract, transform, and load (ETL) data using Java and mesh join logic
- Execute advanced analytical queries to uncover sales trends, product performance, and seasonal patterns

---

## 🗂️ Components

### 1. **Database Schema (`Create-DW.sql`)**
- **Schemas**: `MASTER_DATA`, `METRO_DATAWAREHOUSE`
- **Tables**: `product`, `customer`, `supplier`, `store`, `date`, `sales`
- Implements star schema with referential integrity constraints

### 2. **ETL Engine (`Mesh_Join.java`)**
- Java-based backend to process and merge transactions using a **Mesh Join** algorithm
- Optimizes joins via in-memory partitioning and multi-threaded processing
- Batches final merged data into the DW to minimize I/O overhead

### 3. **Analytical Queries (`Queries-DW.sql`)**
- Revenue trends by season, product affinity, quarterly growth, volatility, outlier detection
- View creation for recurring dashboard-style reports
- SQL constructs used: CTEs, window functions, ROLLUP, CASE, and aggregation

### 4. **Documentation (`Project-Report.docx`)**
- Covers architecture, modeling choices, algorithm explanation, and ETL strategy
- Includes analysis of Mesh Join's pros and cons
- Learning objectives and final conclusions

---

## 🧪 Sample Analytical Insights

- Top 5 products by weekday/weekend revenue (monthly drill-down)
- Store-wise quarterly revenue growth rates
- Supplier contributions by product and store
- Seasonal and half-year product performance
- Product bundling suggestions using affinity analysis

---

## 💻 Technology Stack

- **Database**: MySQL
- **ETL Processing**: Java with Mesh Join logic
- **Data Format**: Relational tables and CSV files
- **Concurrency**: Multi-threaded data processing using Java Queues

---

## 🚧 Limitations of Mesh Join

- High memory footprint
- Potential for uneven partitioning/load imbalance
- Complex implementation and tuning

---

## 📚 Learning Outcomes

- Understand star schema modeling and fact/dimension separation
- Implement Java-based ETL pipelines with memory-optimized joins
- Use SQL to generate business insights from DW data

---

## 🧾 Author & Acknowledgements

**Author**: Hamza Mahmood Burney (22i-2058)  
**Supervisor**: Dr. Asif Naeem  
**Course**: DS3003 - Data Warehousing and Business Analytics  
**Institution**: FAST-NUCES  

---

## 📄 How to Run

1. **Create Database**  
   Execute `Create-DW.sql` in MySQL Workbench or CLI

2. **Run ETL Program**  
   Compile and run `Mesh_Join.java` (Java 8+)

3. **Analyze Data**  
   Run queries from `Queries-DW.sql` on `METRO_DATAWAREHOUSE`

---
