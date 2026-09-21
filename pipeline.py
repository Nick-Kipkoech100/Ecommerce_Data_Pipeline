from pyspark.sql import functions as F
from src.spark_session import create_spark_session
from src.logging_utils import setup_logger
from src.transformation import (
    remove_duplicates,
    normalize_dates, 
    standardize_customer_tier,
    remove_null_keys,
    flag_negative_amounts
)
from src.schemas import (
    orders_schema,
    order_items_schema,
    returns_schema,
    customer_schema,
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


    print("\nNULL-key rows removed:")
    print(f"Orders: {orders_null_keys_removed}")

    print("\nDuplicate rows removed:")
    print(f"Orders: {orders_duplicates_removed}")
    print(f"Returns: {returns_duplicates_removed}")
    print(f"Customers: {customers_duplicates_removed}")
    print(f"Order items: {order_items_duplicates_removed}")

#Rejection Counts for the respective Dataframes
    orders_rejected_count = orders_rejected.count()
    returns_rejected_count = returns_rejected.count()
    customers_rejected_count = customers_rejected.count()
    order_items_rejected_count = order_items_rejected.count()

    print("\nRejected row counts:")

    print(f"Orders rejected: {orders_rejected_count}")
    print(f"Returns rejected: {returns_rejected_count}")
    print(f"Customers rejected: {customers_rejected_count}")
    print(f"Order items rejected: {order_items_rejected_count}")

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

    print("Data ingestion and cleaning completed successfully.")

    # Inspect the orders DataFrame
    print("\nOrders schema:")
    orders_df.printSchema()

    print("\nSample of cleaned orders:")
    orders_df.show(5)


if __name__ == "__main__":
    main()