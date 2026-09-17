from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.types import DateType


def load_csv(spark, file_path, schema):
    """
    Load a CSV using an explicit schema and separate
    malformed records into a rejected DataFrame.
    """

    # 1. Read raw CSV values as strings
    raw_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(file_path)
    )

    cast_columns = []
    rejection_conditions = []
    rejection_reasons = []

    # 2. Build casting expressions and validation conditions
    for field in schema.fields:

        raw_column = F.col(field.name)

        if isinstance(field.dataType, DateType):

            cast_column = F.coalesce(
                F.try_to_date(raw_column, "dd/MM/yyyy"),
                F.try_to_date(raw_column, "yyyy-MM-dd")
            )


        else:

            cast_column = raw_column.cast(
                field.dataType
            )

        cast_columns.append(
            cast_column.alias(field.name)
        )

        # A value is invalid only when:
        # the source contains a value AND
        # that value failed to convert.
        failed_cast = (
            raw_column.isNotNull()
            & (F.trim(raw_column) != "")
            & cast_column.isNull()
        )

        rejection_conditions.append(
            failed_cast
        )
# 3.Combine Rejection reasons
        rejection_reasons.append(
            F.when(
                failed_cast,
                F.lit(
                    f"{field.name}: invalid {field.dataType.simpleString()}"
                )
            )
        )

    # 4. Combine all column validation conditions
    rejection_condition = F.lit(False)

    for condition in rejection_conditions:
        rejection_condition = (
            rejection_condition | condition
        )

    #Combine Rejection reasons 
    rejection_reason = F.concat_ws(
        "; ",
        *rejection_reasons
    )

    # 4. Create the final DataFrame with the target schema
    cast_df = raw_df.select(cast_columns)

    # 5. Separate rejected and valid rows
    rejected_df = (
        raw_df
        .withColumn("rejection_reason", rejection_reason)
        .filter(rejection_condition)
    )

# 6. Filter out rejected rows from the valid DataFrame
    valid_df = cast_df.filter(
        ~rejection_condition
    )

    return valid_df, rejected_df