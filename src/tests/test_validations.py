import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from chispa.dataframe_comparer import assert_df_equality
from validations import (
    check_duplicates,
    check_missing_values,
    check_data_format,
    validate_schema,
)


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("ValidationsTests").getOrCreate()


def test_check_duplicates(spark):
    # Input DataFrame
    data = [("Alice", 30), ("Bob", 25), ("Alice", 30)]
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)

    # Expected DataFrame
    expected_data = [("Alice", 30)]
    expected_df = spark.createDataFrame(expected_data, schema)

    # Run function
    duplicates_df = check_duplicates("test_df", df, ["name", "age"])

    # Assert
    assert_df_equality(
        duplicates_df, expected_df, ignore_row_order=True, ignore_column_order=True
    )


def test_check_missing_values(spark):
    # Input DataFrame
    data = [("Alice", 30), ("Bob", None), ("Charlie", 25)]
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)

    # Run function
    missing_count = check_missing_values("test_df", df, "age")

    # Assert
    assert missing_count == 1


def test_check_data_format(spark):
    # Input DataFrame
    data = [("Alice", "2023-01-01"), ("Bob", "2023-01-02"), ("Charlie", "invalid_date")]
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("date", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)

    # Expected DataFrame
    expected_data = [("Charlie", "invalid_date")]
    expected_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("date", StringType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Run function
    invalid_rows_df = check_data_format("test_df", df, "date", "yyyy-MM-dd")

    # Assert
    assert_df_equality(
        invalid_rows_df, expected_df, ignore_row_order=True, ignore_column_order=True
    )


def test_validate_schema(spark):
    # Input DataFrame
    data = [("Alice", 30), ("Bob", 25)]
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)

    # Expected Schema
    expected_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    # Run function
    result = validate_schema(df, expected_schema)

    # Assert
    assert result is True
