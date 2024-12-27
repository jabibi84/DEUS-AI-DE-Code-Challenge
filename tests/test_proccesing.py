import sys
import os
import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from chispa.dataframe_comparer import assert_df_equality

# Add src to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from src.proccesing import load_csv  # Import your custom function


def test_load_csv(spark):
    csv_path = "tests/sample_data.csv"  # Update the path to the test CSV file
    expected_data = [("Alice", 30), ("Bob", 25)]
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, schema)
    result = load_csv(spark, csv_path, schema)
    assert_df_equality(
        result, expected_df, ignore_row_order=True, ignore_column_order=True
    )
