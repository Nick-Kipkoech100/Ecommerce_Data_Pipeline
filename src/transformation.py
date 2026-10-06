from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.window import Window


def remove_duplicates(df):
    """
    Remove exact duplicate rows from a DataFrame
    and return the number of rows removed.
    """

    rows_before = df.count()

    cleaned_df = df.dropDuplicates()

    rows_after = cleaned_df.count()

    duplicates_removed = rows_before - rows_after

    return cleaned_df, duplicates_removed


def normalize_dates(df):
    """
    Ensure all date columns remain standardized as Spark DateType.
    Spark represents DateType values using ISO format (YYYY-MM-DD).
    """

    for field in df.schema.fields:
        if isinstance(field.dataType, DateType):
            df = df.withColumn(
                field.name,
                F.to_date(F.col(field.name))
            )

    return df


def standardize_customer_tier(df):
    """
    Standardize customer_tier values to lowercase.
    """
    return df.withColumn(
        "customer_tier",
        F.lower(F.col("customer_tier"))
    )



def remove_null_keys(df, key_columns):
    """
    Remove rows where any specified key column is NULL.
    Return the cleaned DataFrame and number of rows removed.
    """

    rows_before = df.count()

    cleaned_df = df.dropna(
        subset=key_columns
    )

    rows_after = cleaned_df.count()

    rows_removed = rows_before - rows_after

    return cleaned_df, rows_removed


def flag_negative_amounts(df):
    """
    Add a boolean flag for orders with a negative total_amount.
    Negative amounts are flagged but not removed.
    """

    return df.withColumn(
        "is_negative_amount",
        F.when(
            F.col("total_amount") < 0,
            F.lit(True)
        ).otherwise(
            F.lit(False)
        )
    )

# Net Amount calcuulation after applying discount percentage
def calculate_net_amount(df):
    """
    Calculate net order amount after applying the discount percentage.
    """

    return df.withColumn(
        "net_amount",
        F.col("total_amount") * (
            1 - F.col("discount_pct") / 100
        )
    )

#Join to Find orphaned order items whose order_id does not exist in the orders DataFrame

def find_orphaned_order_items(order_items_df, orders_df):
    """
    Identify order items whose order_id does not exist
    in the orders DataFrame.

    Returns only the orphaned order items.
    """

    orphaned_items = order_items_df.join(
        orders_df.select("order_id"),
        on="order_id",
        how="left_anti"
    )

    return orphaned_items


def join_orders_customers(orders_df, customers_df):
    return orders_df.join(
        customers_df,
        on="customer_id",
        how="inner"
    )


def join_orders_order_items(orders_df, order_items_df):
    return orders_df.join(
        order_items_df,
        on="order_id",
        how="inner"
    )

def find_orphaned_order_items(order_items_df, orders_df):
    return order_items_df.join(
        orders_df.select("order_id"),
        on="order_id",
        how="left_anti"
    )


def calculate_customer_lifetime_rank(orders_df, customers_df):
    customer_spend = (
        orders_df
        .join(
            customers_df.select("customer_id", "country"),
            on="customer_id",
            how="inner"
        )
        .groupBy("customer_id", "country")
        .agg(
            F.sum("net_amount").alias("lifetime_net_spend")
        )
    )

    country_window = Window.partitionBy("country").orderBy(
        F.col("lifetime_net_spend").desc()
    )

    return customer_spend.withColumn(
        "country_rank",
        F.rank().over(country_window)
    )


def calculate_rolling_order_count(orders_df):
    order_window = (
        Window
        .partitionBy("customer_id")
        .orderBy(F.col("order_date").cast("timestamp").cast("long"))
        .rangeBetween(-7 * 86400, 0)
    )

    return orders_df.withColumn(
        "rolling_7_day_order_count",
        F.count("order_id").over(order_window)
    )


def calculate_category_revenue_share(orders_df, order_items_df):
    category_revenue = (
        order_items_df
        .join(
            orders_df.select("order_id", "order_date"),
            on="order_id",
            how="inner"
        )
        .withColumn(
            "month",
            F.date_trunc("month", F.col("order_date"))
        )
        .withColumn(
            "item_revenue",
            F.col("quantity") * F.col("unit_price")
        )
        .groupBy("month", "category")
        .agg(
            F.sum("item_revenue").alias("category_revenue")
        )
    )

    month_window = Window.partitionBy("month")

    return category_revenue.withColumn(
        "monthly_total_revenue",
        F.sum("category_revenue").over(month_window)
    ).withColumn(
        "revenue_share",
        F.col("category_revenue") / F.col("monthly_total_revenue")
    )