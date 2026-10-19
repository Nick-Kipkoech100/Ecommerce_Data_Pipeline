from src.transformation import(
     remove_null_keys,
     flag_negative_amounts,
     calculate_net_amount
)


def test_remove_null_keys(spark):
    data = [
        ("C001", "Alice"),
        (None, "Bob"),
        ("C003", "Carol"),
    ]

    columns = ["customer_id", "customer_name"]

    df = spark.createDataFrame(data, columns)

    result_df, rows_removed = remove_null_keys(
        df,
        ["customer_id"]
    )

    assert rows_removed == 1
    assert result_df.count() == 2


def test_flag_negative_amounts(spark):
    data = [
        ("O001", 100.00),
        ("O002", -50.00),
        ("O003", 0.00),
    ]

    columns = ["order_id", "total_amount"]

    df = spark.createDataFrame(data, columns)

    result_df = flag_negative_amounts(df)

    results = result_df.select(
        "order_id",
        "is_negative_amount"
    ).collect()

    assert results[0]["is_negative_amount"] is False
    assert results[1]["is_negative_amount"] is True
    assert results[2]["is_negative_amount"] is False

def test_calculate_net_amount(spark):
    data = [
        ("O001", 100.00, 20.00),
        ("O002", 200.00, 10.00),
        ("O003", 50.00, 0.00),
    ]

    columns = ["order_id", "total_amount", "discount_pct"]

    df = spark.createDataFrame(data, columns)

    result_df = calculate_net_amount(df)

    results = result_df.select(
        "order_id",
        "net_amount"
    ).collect()

    assert results[0]["net_amount"] == 80.00
    assert results[1]["net_amount"] == 180.00
    assert results[2]["net_amount"] == 50.00