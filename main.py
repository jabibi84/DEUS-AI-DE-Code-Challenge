import sys

sys.path.append("./src")

from src.proccesing import load_data
from pyspark.sql import SparkSession
from src.utils import get_logger

logger = get_logger(__name__)

def main():
    logger.info("Starting the application...")
    try:
        spark = SparkSession.builder.appName(
            "Deus DE - Challenge Jlondono"
        ).getOrCreate()
        logger.info("SparkSession initialized.")

        sales_df = load_data(spark, "/app/data/input/sales_uuid.csv")
        logger.info("sales.csv Loaded Success..")

        products_df = load_data(spark, "/app/data/input/products_uuid.csv")
        logger.info("Products.csv Loaded Success..")

        stores_df = load_data(spark, "/app/data/input/stores_uuid.csv")
        logger.info("stores.csv Loaded Success..")

    except Exception as e:
        logger.error(f"Application encountered an error: {e}")
    finally:
        logger.info("Application finished.")


if __name__ == "__main__":
    main()
