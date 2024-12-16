import sys

sys.path.append("./src")  # noqa: E402

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.functions import udf  # noqa: E402
from pyspark.sql.types import FloatType, StringType  # noqa: E402

# Importar módulos desde el directorio "src"
from src.cleanning import (  # noqa: E402
    drop_duplicates,
    enforce_dataframe_schema,
    remove_duplicates_by_column,
    standardize_date_format,
)
from src.proccesing import load_csv  # noqa: E402
from src.SparkSchemas import SchemaManager  # noqa: E402
from src.transformations import (  # noqa: E402
    calculate_monthly_sales,
    calculate_total_revenue,
    categorize_price,
    enrich_data,
)
from src.utils import get_config, get_logger, write_dataframe  # noqa: E402
from src.validations import check_duplicates  # noqa: E402

logger = get_logger(__name__)


def main():
    logger.info("Starting the application...")
    try:
        spark = SparkSession.builder.appName(
            "Deus DE - Code Challenge Jlondono"
        ).getOrCreate()
        logger.info("SparkSession initialized.")

        # get configuration
        config = get_config("./config/metadata.json")
        for tbl in config["datasets"]:
            name = tbl["name"]
            path = tbl["file"]
            columns = tbl["columns"]
            # ***************************************************************
            # 1. 1. Data Preparations - load datasets and validate duplicates
            # ***************************************************************
            globals()[f"df_{name}"] = load_csv(spark, path)
            df_data = globals()[f"df_{name}"]
            # *********************************************
            # Identify and handle duplicates (General DF)
            # *********************************************
            df_data = (
                drop_duplicates(df_data)
                if check_duplicates(name, df_data) > 0
                else df_data
            )
            # *******************************************
            # Identify and handle duplicates (By Column)
            # *******************************************
            for field in columns:
                if field.get("unique"):
                    df_data = (
                        drop_duplicates(df_data, field["name"])
                        if check_duplicates(name, df_data, field["name"]) > 0
                        else df_data
                    )
                    df_data = (
                        remove_duplicates_by_column(name, df_data, field["name"])
                        if check_duplicates(name, df_data, field["name"]) > 0
                        else df_data
                    )

            globals()[f"df_{name}"] = df_data

        # Fixing inconsistences data format - Data Cleaning
        df_transactions = standardize_date_format(
            df_SalesTransactions, "transaction_date"  # noqa: F821
        ).drop("transaction_date")

        df_transactions = df_transactions.withColumnRenamed(
            "standardized_date", "transaction_date"
        )

        # Registrar la función como UDF (User Defined Function)
        categorize_price_udf = udf(categorize_price, StringType())
        df_transactions = df_transactions.withColumn(
            "price_category",
            categorize_price_udf(df_transactions["price"].cast(FloatType())),
        )

        df_transactions = enforce_dataframe_schema(
            "TransactionSales",
            df_transactions,
            SchemaManager.get_schema("SalesTransactions"),
        )
        df_Products_clean = enforce_dataframe_schema(
            "Products", df_Products, SchemaManager.get_schema("Products")  # noqa: F821
        )
        df_Stores_clean = enforce_dataframe_schema(
            "Stores", df_Stores, SchemaManager.get_schema("Stores")  # noqa: F821
        )

        # transformations
        df_revenue = calculate_total_revenue(df_transactions, df_Products_clean)
        df_monthly_sales = calculate_monthly_sales(df_transactions, df_Products_clean)  # noqa: F821
        df_enrich = enrich_data(df_transactions, df_Products_clean, df_Stores_clean)

        # output
        write_dataframe(
            df=df_enrich,
            output_path="data/output/sales_product_store",
            format="parquet",
            partition_by=["category", "transaction_date"],
            mode="overwrite",
        )

        write_dataframe(
            df=df_monthly_sales,
            output_path="data/output/df_monthly_sales",
            format="parquet",
            mode="overwrite",
        )

        write_dataframe(
            df=df_revenue,
            output_path="data/output/Revenueinsights",
            format="csv",
            mode="overwrite",
        )

    except Exception as e:
        logger.error(f"Application encountered an error: {e}")
    finally:
        logger.info("Application finished.")


if __name__ == "__main__":
    main()
