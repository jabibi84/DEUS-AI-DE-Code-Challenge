import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
import os
import csv
import shutil
import chispa
from src.proccesing import  load_csv

# Create a Spark session for testing
spark = SparkSession.builder \
    .appName("ChallengeTestApp") \
    .getOrCreate()


def create_mock_data():
    # Create mock data to be used in tests
    data = [
        ("id1", "Product A", "Category X", "6", "2024-12-01", "55.0"),
        ("id2", "Product B", "Category Y", "12", "2024-12-02", "105.0"),
    ]
    columns = ["product_id", "product_name", "category", "quantity", "transaction_date", "price"]

    return spark.createDataFrame(data, columns)


def create_mock_csv(test_path):
    # Define the CSV data
    test_data = [
        ["product_id", "product_name", "category"],
        ["id1", "Product A", "Category X"],
        ["id2", "Product B", "Category Y"]
    ]

    # Ensure the 'tmp' directory exists
    os.makedirs('tmp', exist_ok=True)

    # Write to CSV using the csv module
    with open(test_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(test_data)


def delete_tmp_files():
    # Manually perform cleanup after running tests
    if os.path.exists('tmp'):
        shutil.rmtree('tmp')
        print("Cleaned up test files.")


def test_read_csv():
    # Define path for the CSV test file
    test_path = "tmp/test_products.csv"

    # Create a mock CSV file for testing
    create_mock_csv(test_path)

    # Use the read_csv_file_to_pyspark_dataframe method
    df = load_csv(spark,test_path)

    # Define the expected DataFrame
    expected_data = [
        ("id1", "Product A", "Category X"),
        ("id2", "Product B", "Category Y")
    ]

    expected_columns = ["product_id", "product_name", "category"]

    expected_df = spark.createDataFrame(expected_data, expected_columns)

    # Use Chispa to compare the DataFrames
    chispa.assert_df_equality(df, expected_df, ignore_column_order=True, ignore_nullable=True, ignore_row_order=True)

    # Clean up temporary files
    delete_tmp_files()

