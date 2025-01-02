import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.validations import (
    check_duplicates,
    check_missing_values,
    check_data_format,
    validate_schema,
)

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("ValidationsTests").getOrCreate()


def test_check_duplicates(spark):
    # Esquema
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    # DataFrame de prueba
    data = [
        ("1", "Alice", 30),  # Primera aparición de la fila
        ("2", "Bob", 25),
        ("1", "Alice", 30),  # Duplicado de la primera fila
    ]
    df = spark.createDataFrame(data, schema)

    # Agregar depuración en el test
    total_rows = df.count()
    distinct_rows = df.distinct().count()
    print(f"Total rows: {total_rows}, Distinct rows: {distinct_rows}")

    # Verifica duplicados en todas las columnas
    duplicates_all_columns = check_duplicates("TestDF", df)
    assert (
        duplicates_all_columns == 1
    ), f"Expected 1 duplicate, got {duplicates_all_columns}"

    # Verifica duplicados basados en una columna
    duplicates_column = check_duplicates("TestDF", df, "id")
    assert (
        duplicates_column == 1
    ), f"Expected 1 duplicate in 'id', got {duplicates_column}"


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
    # Esquema
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("age", StringType(), True),
        ]
    )

    # DataFrame de prueba
    data = [("1", "30"), ("2", "not_a_number"), ("3", "45")]
    df = spark.createDataFrame(data, schema)

    # Pruebas
    assert check_data_format("TestDF", df, "age", "int") == 1
    assert check_data_format("TestDF", df, "id", "string") == 0


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
