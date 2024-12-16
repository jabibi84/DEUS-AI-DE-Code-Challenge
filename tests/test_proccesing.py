from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from chispa.dataframe_comparer import assert_df_equality
from proccesing import load_csv


def test_load_csv(spark):
    csv_path = "tests/sample_data.csv"
    expected_data = [("Alice", 30), ("Bob", 25)]
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, schema)
    result = load_csv(spark, csv_path)
    assert_df_equality(
        result, expected_df, ignore_row_order=True, ignore_column_order=True
    )
