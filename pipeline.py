from src.spark_session import create_spark_session


def main():
    spark = create_spark_session()

    print("=" * 50)
    print("Spark Session Started Successfully!")
    print(f"Spark Version: {spark.version}")
    print("=" * 50)

    spark.stop()


if __name__ == "__main__":
    main()