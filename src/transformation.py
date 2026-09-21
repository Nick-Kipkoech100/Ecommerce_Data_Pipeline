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