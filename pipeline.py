from pyspark.sql import functions as F
from src.spark_session import create_spark_session
from src.logging_utils import setup_logger
from src.transformation import (
    remove_duplicates,
    normalize_dates, 
    standardize_customer_tier,
    remove_null_keys,
    flag_negative_amounts,
    calculate_net_amount,
    join_orders_customers,
    join_orders_order_items,
    find_orphaned_order_items,
    calculate_customer_lifetime_rank,
    calculate_rolling_order_count,
    calculate_category_revenue_share,
    flag_refund_exceeds_order,
    calculate_return_rate_by_category,
    calculate_return_rate_by_customer_tier,
    calculate_top_refund_customers,
)
from src.schemas import (
    orders_schema,
    order_items_schema,
    returns_schema,
    customer_schema
)
from src.ingestion import load_csv


def main():
    spark = create_spark_session()

    logger = setup_logger()
    logger.info("Pipeline started")

    print("=" * 50)
    print("Spark Session Started Successfully!")
    print(f"Spark Version: {spark.version}")
    print("=" * 50)


   
    orders_df, orders_rejected = load_csv(
            spark,
            "data/raw/orders.csv",
            orders_schema
        )

    returns_df, returns_rejected = load_csv(
        spark,
        "data/raw/returns.csv",
        returns_schema
    )

    customers_df, customers_rejected = load_csv(
        spark,
        "data/raw/customers.csv",
        customer_schema
    )

    order_items_df, order_items_rejected = load_csv(
        spark,
        "data/raw/order_items.csv",
        order_items_schema
    )

    # Remove exact duplicate rows
    orders_df, orders_duplicates_removed = remove_duplicates(orders_df)
    returns_df, returns_duplicates_removed = remove_duplicates(returns_df)
    customers_df, customers_duplicates_removed = remove_duplicates(customers_df)
    order_items_df, order_items_duplicates_removed = remove_duplicates(order_items_df)

    # Normalize date columns
    orders_df = normalize_dates(orders_df)
    returns_df = normalize_dates(returns_df)
    customers_df = normalize_dates(customers_df)
    order_items_df = normalize_dates(order_items_df)

    # Standardize customer_tier values to lowercase
    customers_df = standardize_customer_tier(customers_df)

    orders_df, orders_null_keys_removed = remove_null_keys(
        orders_df,
        ["order_id", "customer_id"]
    )

    #Flag negative order amounts without removing them
    orders_df = flag_negative_amounts(orders_df)

    #Calculate net_amount for orders
    orders_df = calculate_net_amount(orders_df)


    # Identify orphaned order items
    orphaned_order_items_df = find_orphaned_order_items(
    order_items_df,
    orders_df
    )

    # Write orphaned order items to separate output
    orphaned_order_items_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/orphaned_order_items")

    #Join Orders with customers
    orders_with_customers = join_orders_customers(
    orders_df,
    customers_df
    )

    #Join enriched orders with order items
    enriched_orders = join_orders_order_items(
    orders_with_customers,
    order_items_df
    )

    # Add partition columns for final Parquet output
    enriched_orders = (
        enriched_orders
        .withColumn("year", F.year("order_date"))
        .withColumn("month", F.month("order_date"))
    )

    #Validate orders without matching customers (orphaned orders)
    orphaned_orders = orders_df.join(
    customers_df.select("customer_id"),
    on="customer_id",
    how="left_anti"
    )

    orphaned_order_count = orphaned_orders.count()

    if orphaned_order_count > 0:
        logger.warning(
            f"Orphaned orders detected: {orphaned_order_count}"
        )
    else:
        logger.info("No orphaned orders detected.")

    # Task 4.6: Rank customers by lifetime net spend within each country
    customer_lifetime_rank = calculate_customer_lifetime_rank(
        orders_df,
        customers_df
    )

    # Task 4.7: Calculate 7-day rolling order count per customer
    rolling_order_count = calculate_rolling_order_count(
        orders_df
    )


    # Task 4.8: Calculate each product category's share of monthly revenue
    category_revenue_share = calculate_category_revenue_share(
        orders_df,
        order_items_df
    )


    # Task 5.1: Flag returns where refund exceeds the order net amount
    refund_analysis = flag_refund_exceeds_order(
    returns_df,
    orders_df
    )

    # Task 5.2: Calculate return rate by category
    return_rate_by_category = calculate_return_rate_by_category(
        orders_df,
        order_items_df,
        returns_df
    )

    # Task 5.3: Calculate return rate by customer tier
    return_rate_by_customer_tier = calculate_return_rate_by_customer_tier(
        orders_df,
        customers_df,
        returns_df
    )

    # Task 5.4: Calculate top 10 customers by total refund amount
    top_refund_customers = calculate_top_refund_customers(
        returns_df,
        orders_df
    )


    # Write Task 4 outputs
    customer_lifetime_rank.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv("output/customer_lifetime_rank")

    rolling_order_count.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv("output/rolling_order_count")

    category_revenue_share.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv("output/category_revenue_share")

    #Write Task 5 outputs
    refund_analysis.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv("output/refund_analysis")

    return_rate_by_category.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv("output/return_rate_by_category")

    return_rate_by_customer_tier.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv("output/return_rate_by_customer_tier")

    top_refund_customers.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv("output/top_refund_customers")

    #Write final enriched dataset as partitioned parquet
    enriched_orders.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet("output/enriched_orders")
    

    #Log data-quality metrics
    logger.info(
        f"Orphaned order items: {orphaned_order_items_df.count()}"
    )

    logger.info("\nNull-key rows removed:")
    logger.info(f"Orders: {orders_null_keys_removed}")

    logger.info("\nDuplicate rows removed:")
    logger.info(f"Orders: {orders_duplicates_removed}")
    logger.info(f"Returns: {returns_duplicates_removed}")
    logger.info(f"Customers: {customers_duplicates_removed}")
    logger.info(f"Order items: {order_items_duplicates_removed}")

    logger.info(f"Orphaned orders: {orphaned_order_count}")


    #Rejection Counts for the respective Dataframes
    orders_rejected_count = orders_rejected.count()
    returns_rejected_count = returns_rejected.count()
    customers_rejected_count = customers_rejected.count()
    order_items_rejected_count = order_items_rejected.count()

    print("\nRejected row counts:")

    logger.info(f"Orders rejected: {orders_rejected_count}")
    logger.info(f"Returns rejected: {returns_rejected_count}")
    logger.info(f"Customers rejected: {customers_rejected_count}")
    logger.info(f"Order items rejected: {order_items_rejected_count}")

    if orders_rejected_count > 0:
        logger.warning("Rejected order records detected.")
        orders_rejected.show(truncate=False)

    if returns_rejected_count > 0:
        logger.warning("Rejected return records detected.")
        returns_rejected.show(truncate=False)

    if customers_rejected_count > 0:
        logger.warning("Rejected customer records detected.")
        customers_rejected.show(truncate=False)

    if order_items_rejected_count > 0:
        logger.warning("Rejected order item records detected.")
        order_items_rejected.show(truncate=False)

    logger.info("Data pipeline completed successfully.")


if __name__ == "__main__":
    main()