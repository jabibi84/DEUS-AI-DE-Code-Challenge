import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Fixture for creating a SparkSession."""
    return SparkSession.builder \
        .appName("pytest-pyspark") \
        .master("local[*]") \
        .getOrCreate()
