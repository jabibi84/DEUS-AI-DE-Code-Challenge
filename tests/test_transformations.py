from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)
from chispa.dataframe_comparer import assert_df_equality
from src.transformations import (
    calculate_monthly_sales,
    calculate_total_revenue,
    categorize_price,
    enrich_data,
)


def test_calculate_total_revenue(spark):
    # Input DataFrame
    data = [("Alice", 100), ("Bob", 200), ("Alice", 150)]
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("amount", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)

    # Expected DataFrame
    expected_data = [("Alice", 250), ("Bob", 200)]
    expected_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("total_revenue", IntegerType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Run function
    result = calculate_total_revenue(df)

    # Assert
    assert_df_equality(
        result, expected_df, ignore_row_order=True, ignore_column_order=True
    )


def test_calculate_monthly_sales(spark):
    # Input DataFrame
    data = [("2023-01-01", 100), ("2023-01-15", 150), ("2023-02-01", 200)]
    schema = StructType(
        [
            StructField("date", StringType(), True),
            StructField("sales", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)

    # Expected DataFrame
    expected_data = [("2023-01", 250), ("2023-02", 200)]
    expected_schema = StructType(
        [
            StructField("month", StringType(), True),
            StructField("total_sales", IntegerType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Run function
    result = calculate_monthly_sales(df, "date", "sales")

    # Assert
    assert_df_equality(
        result, expected_df, ignore_row_order=True, ignore_column_order=True
    )


def test_categorize_price(spark):
    # Input DataFrame
    data = [("Item A", 15.0), ("Item B", 50.0), ("Item C", 120.0)]
    schema = StructType(
        [
            StructField("item", StringType(), True),
            StructField("price", FloatType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)

    # Expected DataFrame
    expected_data = [
        ("Item A", 15.0, "Low"),
        ("Item B", 50.0, "Medium"),
        ("Item C", 120.0, "High"),
    ]
    expected_schema = StructType(
        [
            StructField("item", StringType(), True),
            StructField("price", FloatType(), True),
            StructField("price_category", StringType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Run function
    result = categorize_price(df, "price", "price_category")

    # Assert
    assert_df_equality(
        result, expected_df, ignore_row_order=True, ignore_column_order=True
    )


def test_enrich_data(spark):
    # Input DataFrame 1
    sales_data = [
        ("Alice", 100, "2023-01"),
        ("Bob", 200, "2023-01"),
    ]
    sales_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("sales", IntegerType(), True),
            StructField("month", StringType(), True),
        ]
    )
    sales_df = spark.createDataFrame(sales_data, sales_schema)

    # Input DataFrame 2
    demographic_data = [
        ("Alice", 30, "Engineer"),
        ("Bob", 40, "Manager"),
    ]
    demographic_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("occupation", StringType(), True),
        ]
    )
    demographic_df = spark.createDataFrame(demographic_data, demographic_schema)

    # Expected DataFrame
    expected_data = [
        ("Alice", 100, "2023-01", 30, "Engineer"),
        ("Bob", 200, "2023-01", 40, "Manager"),
    ]
    expected_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("sales", IntegerType(), True),
            StructField("month", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("occupation", StringType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Run function
    result = enrich_data(sales_df, demographic_df, "name")

    # Assert
    assert_df_equality(
        result, expected_df, ignore_row_order=True, ignore_column_order=True
    )
