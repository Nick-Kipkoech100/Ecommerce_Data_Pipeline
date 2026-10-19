# E-Commerce Data Pipeline

A PySpark ETL pipeline that processes synthetic e-commerce data, performs data cleaning, enrichment, analytics, and exports optimized Parquet datasets.

**Status:** 🚧 In Development  
### B4 — Execution Plan Analysis

The formatted physical plan for `calculate_category_revenue_share()` shows that Spark reads and prepares the `order_items` and `orders` datasets before performing an inner `BroadcastHashJoin`. Spark broadcasts the orders side of the join, avoiding a full shuffle of both datasets.

The plan then calculates item-level revenue and performs a distributed aggregation by month and category. The aggregation involves an exchange/shuffle to redistribute data across partitions. A second exchange and sort occur before the window calculation because the monthly revenue total is computed using `Window.partitionBy("month")`. The final projection calculates each category's share of monthly revenue.

This plan demonstrates how Spark translates the DataFrame transformations into distributed operations such as joins, exchanges, aggregations, sorting, and window processing.