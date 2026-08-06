import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession


def create_spark_session():
    """
    Create and return a Spark Session.
    """

    spark = (
        SparkSession.builder
        .appName("Ecommerce Data Pipeline")
        .master("local[*]")
        .getOrCreate()
    )

    return spark