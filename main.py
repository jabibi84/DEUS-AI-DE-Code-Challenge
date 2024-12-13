import sys

sys.path.append("./src")
from pyspark.sql.functions import udf, cast
from pyspark.sql.types import StringType, FloatType
from src.SparkSchemas import SchemaManager
from src.proccesing import load_data
from src.validations import check_missing_values, check_data_format, check_duplicates
from src.cleanning import (
    standardize_date_format,
    drop_duplicates_by_column,
    enforce_dataframe_schema,
)
from src.transformations import (
    calculate_total_revenue,
    calculate_monthly_sales,
    enrich_data,
    categorize_price,
    write_dataframe,
)
from pyspark.sql import SparkSession
from src.utils import get_logger, get_config

logger = get_logger(__name__)


def main():
    logger.info("Starting the application...")
    try:
        spark = SparkSession.builder.appName(
            "Deus DE - Challenge Jlondono"
        ).getOrCreate()
        logger.info("SparkSession initialized.")

        # get configuration
        config = get_config("./config/metadata.json")
        for tbl in config["datasets"]:
            name = tbl["name"]
            path = tbl["file"]
            columns = tbl["columns"]
            schema = SchemaManager.get_schema(name)
            # logger.info(schema)

            # load datasets and validate datasets
            df_data = load_data(spark, path)
            globals()[f"df_{name}"] = load_data(spark, path)
            logger.info(f"Loaded dataset: {name}  path: {path}")
            check_duplicates(name, df_data)
            for field in columns:
                check_missing_values(name, df_data, field["name"])
                check_data_format(name, df_data, field["name"], field["type"])
                if field.get("unique"):
                    check_duplicates(name, df_data, field["name"])

        # data cleanning
        df_transactions = standardize_date_format(
            df_SalesTransactions, "transaction_date"
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
        df_transactions.show()

        # transformations
        df_revenue = calculate_total_revenue(df_transactions, df_Products)

        df_monthly_sales = calculate_monthly_sales(df_transactions, df_Products)

        df_enrich = enrich_data(df_transactions, df_Products, df_Stores)

        # output
        write_dataframe(
            df=df_enrich,
            output_path="data/output/sales_product_store",
            format="parquet",
            partition_by=["category", "transaction_date"],
            mode="overwrite",
        )

        write_dataframe(
            df=df_revenue,
            output_path="data/output/Revenueinsights",
            format="csv",
            mode="overwrite",
        )

        write_dataframe(
            df=df_transactions,
            output_path="data/output/transactions",
            format="csv",
            mode="overwrite",
        )

    except Exception as e:
        logger.error(f"Application encountered an error: {e}")
    finally:
        logger.info("Application finished.")


if __name__ == "__main__":
    main()
