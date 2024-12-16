from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from chispa.dataframe_comparer import assert_df_equality
from cleanning import (
    drop_duplicates,
    enforce_dataframe_schema,
    remove_duplicates_by_column,
    standardize_date_format,
)


def test_drop_duplicates(spark):
    data = [("Alice", 30), ("Bob", 25), ("Alice", 30)]
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)
    expected_data = [("Alice", 30), ("Bob", 25)]
    expected_df = spark.createDataFrame(expected_data, schema)

    result = drop_duplicates("test_df", df)
    assert_df_equality(
        result, expected_df, ignore_row_order=True, ignore_column_order=True
    )


def test_enforce_dataframe_schema(spark):
    data = [("Alice", "30"), ("Bob", "25")]
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", StringType(), True),
        ]
    )
    expected_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)
    expected_data = [("Alice", 30), ("Bob", 25)]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    result = enforce_dataframe_schema(df, expected_schema)
    assert_df_equality(
        result, expected_df, ignore_row_order=True, ignore_column_order=True
    )


def test_remove_duplicates_by_column(spark):
    data = [("Alice", 30), ("Bob", 25), ("Alice", 28)]
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)
    expected_data = [("Alice", 30), ("Bob", 25)]
    expected_df = spark.createDataFrame(expected_data, schema)

    result, removed_count = remove_duplicates_by_column("test_df", df, "name")
    assert_df_equality(
        result, expected_df, ignore_row_order=True, ignore_column_order=True
    )
    assert removed_count == 1


def test_standardize_date_format(spark):
    data = [("Alice", "01-01-2023"), ("Bob", "02-01-2023")]
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("date", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)
    expected_data = [("Alice", "2023-01-01"), ("Bob", "2023-01-02")]
    expected_schema = schema
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    result = standardize_date_format(df, "date", "MM-dd-yyyy", "yyyy-MM-dd")
    assert_df_equality(
        result, expected_df, ignore_row_order=True, ignore_column_order=True
    )
