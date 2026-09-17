from pyspark.sql import functions as F
from src.spark_session import create_spark_session
from src.logging_utils import setup_logger
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

    raw_orders = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv("data/raw/orders.csv")
    )

    print("\nOrder date format distribution:")

    raw_orders.select(
        F.when(
            F.col("order_date").rlike(r"^\d{2}/\d{2}/\d{4}$"),
            "DD/MM/YYYY"
        )
        .when(
            F.col("order_date").rlike(r"^\d{4}-\d{2}-\d{2}$"),
            "YYYY-MM-DD"
        )
        .when(
            F.col("order_date").isNull() |
            (F.trim(F.col("order_date")) == ""),
            "NULL/EMPTY"
        )
        .otherwise("OTHER")
        .alias("date_format")
    ).groupBy("date_format").count().show()

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

#Rejection Counts for the respective dataframes
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

    print("Data ingestion completed successfully.")
     # Inspect the orders DataFrame
    print("\nOrders schema:")
    orders_df.printSchema()

    print("\nAbout to display orders...")
    orders_df.show(5)

    print("\nOrders displayed successfully!")

from src.logging_utils import setup_logger

if __name__ == "__main__":
    main()