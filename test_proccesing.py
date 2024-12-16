import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from chispa.dataframe_comparer import assert_df_equality
from src.proccesing import load_csv


@pytest.fixture(scope="module")
def spark():
    """Fixture para inicializar una SparkSession."""
    return SparkSession.builder.master("local[1]").appName("Testing").getOrCreate()


def test_load_csv_valid_file(spark, tmp_path):
    """Prueba para cargar un archivo CSV válido."""
    # Crear datos de prueba
    data = "name,age\nAlice,30\nBob,25"
    csv_path = tmp_path / "test_valid.csv"

    # Escribir el archivo CSV
    with open(csv_path, "w") as f:
        f.write(data)

    # Especificar el esquema esperado
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", StringType(), True),
        ]
    )

    # Cargar el archivo CSV
    df = load_csv(spark, str(csv_path), schema)

    # Crear un DataFrame esperado
    expected_data = [("Alice", "30"), ("Bob", "25")]
    expected_df = spark.createDataFrame(expected_data, schema=schema)

    # Comparar los DataFrames
    assert_df_equality(df, expected_df, ignore_row_order=True)
