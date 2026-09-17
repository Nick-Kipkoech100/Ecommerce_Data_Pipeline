from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
)

#Orders Schema
orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("discount_pct", DoubleType(), True),
])

# Order Items Schema

order_items_schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("category", StringType(), True),
])

#Returns Schema
returns_schema = StructType([
    StructField("return_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("return_date", DateType(), True),
    StructField("reason", StringType(), True),
    StructField("refund_amount", DoubleType(), True),
])

#Customers Schema

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("signup_date", DateType(), True),
    StructField("country", StringType(), True),
    StructField("customer_tier", StringType(), True),
    StructField("email", StringType(), True)
])