import sys
import os
import datetime
from pyspark.sql import SparkSession

# Add src to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    DoubleType,
    DateType,
)
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.functions import udf
from src.transformations import (
    calculate_monthly_sales,
    calculate_total_revenue,
    categorize_price,
    enrich_data,
)
from src.SparkSchemas import SchemaManager


def test_calculate_total_revenue(spark):
    # Datos de entrada
    sales_data = [
        ("1", "1", "P1", 10.0, None, 2.0),
        ("2", "1", "P1", 15.0, None, 3.0),
        ("3", "2", "P2", 5.0, None, 5.0),
    ]
    products_data = [
        ("P1", "Product A", "Category A"),
        ("P2", "Product B", "Category B"),
    ]

    # DataFrames de prueba
    sales_df = spark.createDataFrame(
        sales_data, SchemaManager.get_schema("SalesTransactions")
    )
    products_df = spark.createDataFrame(
        products_data, SchemaManager.get_schema("Products")
    )

    # Resultado esperado
    expected_data = [
        ("1", "Category A", 65.0),  # 10*2 + 15*3
        ("2", "Category B", 25.0),  # 5*5
    ]
    expected_schema = StructType(
        [
            StructField("store_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("total_revenue", DoubleType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Prueba
    result_df = calculate_total_revenue(sales_df, products_df)
    assert_df_equality(result_df, expected_df, ignore_row_order=True)


def test_calculate_monthly_sales(spark):
    # transaction_id, store_id, product_id, quantity, transaction_date, price
    # Datos de entrada
    sales_data = [
        ("1", "1", "P1", 10.0, datetime.date(2024, 12, 1), None),
        ("2", "1", "P1", 15.0, datetime.date(2024, 12, 15), None),
        ("3", "2", "P2", 5.0, datetime.date(2024, 11, 1), None),
    ]

    products_data = [
        ("P1", "Product A", "Category A"),
        ("P2", "Product B", "Category B"),
    ]

    # DataFrames de prueba
    sales_df = spark.createDataFrame(
        sales_data, SchemaManager.get_schema("SalesTransactions")
    )
    sales_df = sales_df.withColumn(
        "transaction_date", sales_df["transaction_date"].cast(DateType())
    )

    products_df = spark.createDataFrame(
        products_data, SchemaManager.get_schema("Products")
    )

    # Resultado esperado
    expected_data = [
        (2024, 12, "Category A", 25.0),  # 10+15
        (2024, 11, "Category B", 5.0),  # 5
    ]
    expected_schema = StructType(
        [
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("category", StringType(), True),
            StructField("total_quantity_sold", DoubleType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Prueba
    result_df = calculate_monthly_sales(sales_df, products_df)
    assert_df_equality(result_df, expected_df, ignore_row_order=True)


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

    # Registrar la función como UDF (User Defined Function)
    categorize_price_udf = udf(categorize_price, StringType())
    result = df.withColumn(
        "price_category",
        categorize_price_udf(df["price"].cast(FloatType())),
    )

    # Assert
    assert_df_equality(
        result, expected_df, ignore_row_order=True, ignore_column_order=True
    )


def test_enrich_data(spark):
    # Datos de entrada
    sales_data = [
        ("T1", "1", "P1", 10.0, datetime.date(2024, 12, 1), None),
    ]
    products_data = [
        ("P1", "Product A", "Category A"),
    ]
    stores_data = [
        ("1", "Store A", "Location A"),
    ]

    # DataFrames de prueba
    sales_df = spark.createDataFrame(
        sales_data, SchemaManager.get_schema("SalesTransactions")
    )
    products_df = spark.createDataFrame(
        products_data, SchemaManager.get_schema("Products")
    )
    stores_df = spark.createDataFrame(stores_data, SchemaManager.get_schema("Stores"))

    # Resultado esperado
    expected_data = [
        (
            "T1",
            "Store A",
            "Location A",
            "Product A",
            "Category A",
            10.0,
            datetime.date(2024, 12, 1),
            None,
        ),
    ]
    expected_schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("store_name", StringType(), True),
            StructField("location", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("quantity", FloatType(), True),
            StructField("transaction_date", DateType(), True),
            StructField("price", FloatType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Prueba
    result_df = enrich_data(sales_df, products_df, stores_df)
    assert_df_equality(result_df, expected_df, ignore_row_order=True)
