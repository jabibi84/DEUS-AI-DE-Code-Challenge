import pytest
from src.utils import get_config, get_logger, write_dataframe
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from chispa.dataframe_comparer import assert_df_equality


def test_get_logger():
    logger = get_logger("test_logger", "DEBUG")
    assert logger.name == "test_logger"


def test_write_dataframe(spark):
    data = [("Alice", 30), ("Bob", 25)]
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)
    output_path = "tests/output"

    write_dataframe(df, output_path, format="parquet")

    # Reload to confirm write
    reloaded_df = spark.read.parquet(output_path)
    assert_df_equality(df, reloaded_df, ignore_row_order=True, ignore_column_order=True)
