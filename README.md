# Data Analytics & Warehouse Solution
## Executive Summary
This project delivers a comprehensive data analysis platform designed to transform raw business data into actionable insights. While the primary output is high-level business intelligence, the backbone of the project is a robust SQL Server warehouse utilizing the Medallion Architecture.

## Data Analysis & Insights
The primary goal of this repository is to solve business problems through data. Using advanced SQL, I’ve developed an analytical layer that provides:
1. Growth Metrics: YoY (Year-over-Year) and MoM (Month-over-Month) revenue trends using Window Functions.
2. Customer Segmentation: Identifying high-value customers through RFM (Recency, Frequency, Monetary) analysis.
3. Operational Efficiency: Detecting bottlenecks in the supply chain by calculating lead-time variances.

## The "Under the Hood"
To enable the analysis above, I implemented a full-scale data engineering pipeline. This demonstrates a mastery of the following SQL domains:

1. Architectural Design (Medallion Pattern)
To ensure "Data Trust," I built a tiered pipeline:
- **Bronze**: Raw data ingestion (CSV to SQL)
- **Silver**: Data cleansing and deduplication using CTEs and Null handling 
- **Gold**: Final Star Schema modeling with Fact and Dimension tables for optimized query performance.

2. Advanced Technical Implementation
This project incorporates advanced concepts from my specialized SQL training:
Performance Optimization: Strategic use of CTAS and Temp Tables for efficient data processing.
Automation: Stored Procedures to orchestrate the movement of data between layers.
Complex Logic: CASE Statements for dynamic bucketing and Window Ranking (ROW_NUMBER, RANK) for "Top N" reporting.
