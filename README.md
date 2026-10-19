# E-Commerce Data Pipeline

A PySpark ETL pipeline that processes synthetic e-commerce data, performs data cleaning, enrichment, analytics, and exports optimized Parquet datasets.

**Status:** 🚧 In Development  

### B2 — Broadcast Join Optimization

The `orders` and `customers` datasets are joined on `customer_id`. Since the `customers` dataset is relatively small, the join was optimized by explicitly broadcasting `customers_df` using Spark's `broadcast()` function.

This allows Spark to distribute the smaller customers dataset to the workers instead of requiring it to be shuffled across the cluster during the join.

The pipeline was rerun successfully after the change, and the formatted execution plan confirmed that Spark used a `BroadcastHashJoin` for the orders-to-customers join. The transformation output remained unchanged, so the optimization affects execution strategy rather than business logic.

### B4 — Execution Plan Analysis

The formatted physical plan for `calculate_category_revenue_share()` shows that Spark reads and prepares the `order_items` and `orders` datasets before performing an inner `BroadcastHashJoin`. Spark broadcasts the orders side of the join, avoiding a full shuffle of both datasets.

The plan then calculates item-level revenue and performs a distributed aggregation by month and category. The aggregation involves an exchange/shuffle to redistribute data across partitions. A second exchange and sort occur before the window calculation because the monthly revenue total is computed using `Window.partitionBy("month")`. The final projection calculates each category's share of monthly revenue.

This plan demonstrates how Spark translates the DataFrame transformations into distributed operations such as joins, exchanges, aggregations, sorting, and window processing.