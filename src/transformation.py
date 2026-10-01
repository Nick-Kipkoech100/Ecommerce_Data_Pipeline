from pyspark.sql import functions as F
from pyspark.sql.types import DateType


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